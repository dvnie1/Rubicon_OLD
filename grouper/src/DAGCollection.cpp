#include "DAGCollection.hpp"

#include <iostream>
#include <fstream>
#include <chrono>

#include "TransferAnalysis.hpp"
#include "UniqueRandomU64.hpp"
#include "../../tcp_stuff/include/Utility.hpp"
#include "WorkRequest.pb.h"
#include "ColumnSizes.hpp"
#include "Logger.hpp"

DagCollection::DagCollection(tuddbs::TCPServer& server, lb::AdaptiveLoadBalancer& loadBalancer, uint64_t threshold, float maxOverhead)
    : server_(server),
      load_balancer_(loadBalancer),
      threshold_(threshold),
      groupingConfig_{.maxMergeOverhead = maxOverhead},
      hwConfig_{},
      worker_thread_(&DagCollection::run, this)
{
}

DagCollection::~DagCollection() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
        sealed_ = true;
    }
    cv_.notify_one();

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void DagCollection::add(plan::PlanDAG&& dag) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (sealed_) {
        throw std::runtime_error("Cannot add DAG to sealed collection");
    }

    dags_.push_back(std::move(dag));
}

void DagCollection::seal() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        sealed_ = true;
    }
    cv_.notify_one();
}

bool DagCollection::is_sealed() const {
    return sealed_.load();
}

const std::vector<plan::PlanDAG>& DagCollection::dags() const {
    return dags_;
}

void DagCollection::run() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return sealed_.load(); });
    }

    if (shutdown_) {
        return;
    }

    analyze();
}

void DagCollection::sendPlansImmediately() {
    for (const auto& dag: dags_) {
        WorkRequest request;
        *request.mutable_queryplan() = dag.getPlan();

        tuddbs::TCPMetaInfo info;
        info.package_type = TcpPackageType::QUERY_PLAN;
        info.payload_size = request.ByteSizeLong();
        info.src_uuid     = dag.getSourceUuid();
        info.tgt_uuid     = dag.getTargetUuid();

        void* out_mem = malloc(info.bytesize());
        tuddbs::Utility::serializeItemToMemory(out_mem, request, info);

        tuddbs::ClientInfo* target = server_.getClientByUuid(info.tgt_uuid);
        if (target != nullptr) {
            server_.sendTo(target, reinterpret_cast<const char*>(out_mem), info.bytesize());
            server_.sendToAllOfType(UnitType::MONITOR_UNIT, reinterpret_cast<const char*>(out_mem), info.bytesize());
        } else {
            std::cerr << "Error: Target with UUID " << info.tgt_uuid << " not found." << std::endl;
        }

        free(out_mem);
    }
}

// Add these includes at the top of the file (if not already present)
#include <chrono>
#include <fstream>

void DagCollection::analyze() {
    VLOG("Analyzing collection with " << dags_.size() << " DAG(s)");

#ifdef PROFILING_ENABLED
    const size_t numDags = dags_.size();
    using Clock = std::chrono::high_resolution_clock;
    auto tStart     = Clock::now();
    auto tStepBegin = tStart;

    auto elapsed = [&]() -> double {
        auto now = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(now - tStepBegin).count();
        tStepBegin = now;
        return ms;
    };
#endif

    std::vector<const plan::BasColMap*>     maps;
    std::vector<const plan::BasColTypeMap*> typeMaps;
    maps.reserve(dags_.size());
    typeMaps.reserve(dags_.size());

    auto schema = getColumnSizes(SCALE_FACTOR);

    for (const auto& dag : dags_) {
        maps.push_back(&dag.getBaseColumns());
        typeMaps.push_back(&dag.getBaseColumnTypes());
    }

#ifdef PROFILING_ENABLED
    double tGatherMaps = elapsed();
#endif

    auto groups = analysis::groupBySupersetAbsorption(maps, groupingConfig_);

#ifdef PROFILING_ENABLED
    double tGrouping = elapsed();
#endif

    auto cuUuids = server_.getUuidForUnitType(UnitType::COMPUTE_UNIT);
    if (cuUuids.empty()) {
        std::cerr << "Error: No COMPUTE_UNIT connected. Cannot dispatch groups." << std::endl;
        return;
    }

    auto plannerUuids = server_.getUuidForUnitType(UnitType::QUERY_PLANER);
    if (plannerUuids.empty()) {
        std::cerr << "Error: No QUERY_PLANER connected. Cannot dispatch groups." << std::endl;
        return;
    }

#ifdef PROFILING_ENABLED
    double tUuidLookup = elapsed();
#endif

    auto transferDecisions = transfer_analysis::decideTransfers(groups, dags_, typeMaps, schema, cuUuids, CHUNK_SIZE, hwConfig_);

#ifdef PROFILING_ENABLED
    double tTransferDecisions = elapsed();
#endif

    std::vector<uint64_t> available_cus;
    available_cus.reserve(cuUuids.size());
    for (const auto& [name, uuid] : cuUuids) {
        available_cus.push_back(uuid);
    }

    std::unordered_map<std::string, std::string> replacement_map;
    std::unordered_map<std::string, WorkItem> column_transfers;

    size_t groupIdx = 0;
    std::vector<char> out_buf;
    for (const auto& group : groups) {
        const uint64_t id = UniqueRandomU64::instance().next();
        const std::string idPrefix = std::to_string(id) + "_";

        std::vector<const plan::PlanDAG*> group_dags;
        group_dags.reserve(group.size());
        for (size_t dagIdx : group) {
            group_dags.push_back(&dags_[dagIdx]);
        }

        uint64_t target_cu = load_balancer_.assignGroup(id, group_dags, available_cus);

        QueryGroup query_group;
        query_group.set_groupid(id);

        const auto& transfersForGroup = transferDecisions[groupIdx];

        replacement_map.clear();
        column_transfers.clear();

        for (size_t dagIdx : group) {
            const plan::BasColMap&     map     = *maps[dagIdx];
            const plan::BasColTypeMap& typeMap = *typeMaps[dagIdx];

            for (const auto& [colName, _] : map) {
                if (column_transfers.contains(colName) || !transfersForGroup.contains(colName)) continue;

                auto pos = colName.find(".");
                if (pos == std::string::npos) {
                    std::cerr << "Invalid column name format: " << colName << std::endl;
                    continue;
                }

                const std::string tableName  = colName.substr(0, pos);
                const std::string columnName = colName.substr(pos + 1);
                const ColumnType  colType    = typeMap.at(colName);

                std::string newTabName = idPrefix + tableName;
                replacement_map.emplace(colName, newTabName);

                VLOG("Transfer: " << colName << " -> " << std::to_string(id) << "_" << tableName);

                WorkItem witem;

                ColumnMessage* sourceCol = witem.mutable_transferdata()->mutable_source();
                sourceCol->set_tabname(tableName);
                sourceCol->set_colname(columnName);
                sourceCol->set_coltype(colType);
                sourceCol->set_isbase(true);

                ColumnMessage* destCol = witem.mutable_transferdata()->mutable_destination();
                destCol->set_tabname(newTabName);
                destCol->set_colname(columnName);
                destCol->set_coltype(colType);
                destCol->set_isbase(true);

                column_transfers.emplace(colName, std::move(witem));
            }

            *query_group.add_plans() = renameTableNames(dags_[dagIdx].getPlan(), replacement_map);
        }

        for (auto& [_, transfer] : column_transfers) {
            *query_group.add_columntransfers() = std::move(transfer);
        }

        WorkRequest request;
        *request.mutable_querygroup() = std::move(query_group);

        tuddbs::TCPMetaInfo info;
        info.package_type = TcpPackageType::QUERY_GROUP;
        info.payload_size = request.ByteSizeLong();
        info.src_uuid     = plannerUuids[0].second;
        info.tgt_uuid     = target_cu;

        out_buf.resize(info.bytesize());
        tuddbs::Utility::serializeItemToMemory(out_buf.data(), request, info);

        tuddbs::ClientInfo* target = server_.getClientByUuid(info.tgt_uuid);
        if (target != nullptr) {
            VLOG("Sending group " << groupIdx << " (" << column_transfers.size() << " transfers, " << query_group.plans_size() << " plans)");
            server_.sendTo(target, reinterpret_cast<const char*>(out_buf.data()), info.bytesize());
            server_.sendToAllOfType(UnitType::MONITOR_UNIT, reinterpret_cast<const char*>(out_buf.data()), info.bytesize());
        } else {
            std::cerr << "Error: Target with UUID " << info.tgt_uuid << " not found." << std::endl;
        }

        ++groupIdx;
    }

#ifdef PROFILING_ENABLED
    double tBuildAndDispatch = elapsed();
    double tTotal = std::chrono::duration<double, std::milli>(Clock::now() - tStart).count();

    const std::string csvPath = "analyze_profile.csv";
    bool fileExists = std::ifstream(csvPath).good();
    std::ofstream csv(csvPath, std::ios::app);
    if (!fileExists) {
        csv << "num_dags,gather_maps_ms,grouping_ms,uuid_lookup_ms,transfer_decisions_ms,build_and_dispatch_ms,total_ms\n";
    }
    csv << numDags << ","
        << tGatherMaps << ","
        << tGrouping << ","
        << tUuidLookup << ","
        << tTransferDecisions << ","
        << tBuildAndDispatch << ","
        << tTotal << "\n";
    csv.flush();
#endif
}

QueryPlan& DagCollection::renameTableNames(
    QueryPlan& plan,
    const std::unordered_map<std::string, std::string>& replacement_map)
{
    std::string comb;
    comb.reserve(64);

    auto tryReplace = [&](const std::string& tabname, const std::string& colname, auto* mutable_column) {
        comb.clear();
        comb += tabname;
        comb += '.';
        comb += colname;
        if (auto it = replacement_map.find(comb); it != replacement_map.end()) {
            mutable_column->set_tabname(it->second);
        }
    };

    for (int i = 0; i < plan.planitems().size(); ++i) {
        WorkItem* witem = plan.mutable_planitems(i);
        switch (witem->opData_case()) {
            case WorkItem::OpDataCase::kFilterData: {
                const auto& col = witem->filterdata().inputcolumn();
                tryReplace(col.tabname(), col.colname(), witem->mutable_filterdata()->mutable_inputcolumn());
            } break;
            case WorkItem::OpDataCase::kJoinData: {
                const auto& inner = witem->joindata().innercolumn();
                tryReplace(inner.tabname(), inner.colname(), witem->mutable_joindata()->mutable_innercolumn());
                const auto& outer = witem->joindata().outercolumn();
                tryReplace(outer.tabname(), outer.colname(), witem->mutable_joindata()->mutable_outercolumn());
            } break;
            case WorkItem::OpDataCase::kAggData: {
                const auto& col = witem->aggdata().inputcolumn();
                tryReplace(col.tabname(), col.colname(), witem->mutable_aggdata()->mutable_inputcolumn());
            } break;
            case WorkItem::OpDataCase::kSortData: {
                const auto& sort = witem->sortdata();
                for (int j = 0; j < sort.inputcolumns_size(); ++j) {
                    const auto& col = sort.inputcolumns(j);
                    tryReplace(col.tabname(), col.colname(), witem->mutable_sortdata()->mutable_inputcolumns(j));
                }
            } break;
            case WorkItem::OpDataCase::kMapData: {
                const auto& col = witem->mapdata().inputcolumn();
                tryReplace(col.tabname(), col.colname(), witem->mutable_mapdata()->mutable_inputcolumn());
                if (witem->mapdata().has_partnercolumn()) {
                    const auto& partner = witem->mapdata().partnercolumn();
                    tryReplace(partner.tabname(), partner.colname(), witem->mutable_mapdata()->mutable_partnercolumn());
                }
            } break;
            case WorkItem::OpDataCase::kMaterializeData: {
                const auto& idx = witem->materializedata().indexcolumn();
                tryReplace(idx.tabname(), idx.colname(), witem->mutable_materializedata()->mutable_indexcolumn());
                const auto& flt = witem->materializedata().filtercolumn();
                tryReplace(flt.tabname(), flt.colname(), witem->mutable_materializedata()->mutable_filtercolumn());
            } break;
            case WorkItem::OpDataCase::kSetData: {
                const auto& inner = witem->setdata().innercolumn();
                tryReplace(inner.tabname(), inner.colname(), witem->mutable_setdata()->mutable_innercolumn());
                const auto& outer = witem->setdata().outercolumn();
                tryReplace(outer.tabname(), outer.colname(), witem->mutable_setdata()->mutable_outercolumn());
            } break;
            case WorkItem::OpDataCase::kFetchData: {
                const auto& col = witem->fetchdata().inputcolumn();
                tryReplace(col.tabname(), col.colname(), witem->mutable_fetchdata()->mutable_inputcolumn());
            } break;
            case WorkItem::OpDataCase::kMultiGroupData: {
                const auto& mg = witem->multigroupdata();
                for (int j = 0; j < mg.groupcolumns_size(); ++j) {
                    const auto& col = mg.groupcolumns(j);
                    tryReplace(col.tabname(), col.colname(), witem->mutable_multigroupdata()->mutable_groupcolumns(j));
                }
                if (mg.has_aggregationcolumn()) {
                    const auto& agg = mg.aggregationcolumn();
                    tryReplace(agg.tabname(), agg.colname(), witem->mutable_multigroupdata()->mutable_aggregationcolumn());
                }
            } break;
            case WorkItem::OpDataCase::kResultData: {
                const auto& res = witem->resultdata();
                for (int j = 0; j < res.resultcolumns_size(); ++j) {
                    const auto& col = res.resultcolumns(j);
                    tryReplace(col.tabname(), col.colname(), witem->mutable_resultdata()->mutable_resultcolumns(j));
                }
                if (res.has_resultindex()) {
                    const auto& idx = res.resultindex();
                    tryReplace(idx.tabname(), idx.colname(), witem->mutable_resultdata()->mutable_resultindex());
                }
            } break;
            default:
                break;
        }
    }
    return plan;
}
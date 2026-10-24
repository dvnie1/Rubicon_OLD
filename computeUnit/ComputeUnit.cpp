#include <ConfigurationRequests.pb.h>
#include <NetworkRequests.pb.h>
#include <QueryPlan.pb.h>
#include <UnitDefinition.pb.h>
#include <WorkItem.pb.h>
#include <WorkRequest.pb.h>
#include <WorkResponse.pb.h>
#include "WorkerCount.pb.h"
#include <numa.h>

#include <infrastructure/ArgParser.hpp>
#include "TCPClient.hpp"
#include "Utility.hpp"
#include <Logger.hpp>
#include <algorithm>
#include <bitset>
#include <chrono>
#include <format>
#include <functional>
#include <infrastructure/DataCatalog.hpp>
#include <infrastructure/DataReader.hpp>
#include <infrastructure/OperatorDispatcher.hpp>
#include <infrastructure/PlanOrchestrator.hpp>
#include <infrastructure/SupportedOperators.hpp>
#include <infrastructure/WorkerPool.hpp>
#include <iostream>
#include <operators/FilterOperator.hpp>
#include <operators/TestDataOperator.hpp>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>

#include "infrastructure/MonitorClient.hpp"

class IdleTestOp : public AbstractOperator {
   public:
    IdleTestOp(WorkItem p_witem) : AbstractOperator(p_witem) {};
    ~IdleTestOp() {
        LOG_DEBUG1("[IdleTestOp] Bye bye bye." << std::endl;)
    }

    WorkResponse run() {
        LOG_DEBUG1("[IdleTestOp] Starting IdleOp." << std::endl;)
        WorkResponse response;
        response.set_info("Yop.");

        const size_t WAIT_TIME = 30;
        auto t_s = std::chrono::high_resolution_clock::now();
        auto t_e = std::chrono::high_resolution_clock::now();
        auto get_dur = [&t_s, &t_e]() -> size_t { return std::chrono::duration_cast<std::chrono::seconds>(t_e - t_s).count(); };
        while (get_dur() < WAIT_TIME) {
            t_e = std::chrono::high_resolution_clock::now();
            continue;
        }

        return response;
    }
};


void send_worker_count(tuddbs::TCPClient *client, int worker_count) {
    using namespace tuddbs;
    LOG_DEBUG1("[ComputeUnit] Sending worker count." << std::endl;)
    lb::WorkerCount wc;
    wc.set_worker_count(worker_count);

    TCPMetaInfo info;
    info.package_type = TcpPackageType::WORKER_COUNT_UPDATE;
    info.payload_size = wc.ByteSizeLong();
    info.src_uuid = client->getUuid();
    void* out_mem = malloc(sizeof(TCPMetaInfo) + info.payload_size);
    if (!out_mem) {
        std::cerr << "[ComputeUnit::send_worker_count] Wasn't able to allocate the requested memory!" << std::endl;
        exit(-1);
    }

    const size_t message_size = tuddbs::Utility::serializeItemToMemory(out_mem, wc, info);

    client->notifyHost(out_mem, message_size);
    free(out_mem);
}


int main(int argc, char** argv) {
    ArgParser parser(argc, argv);
    const size_t cpus_per_node = numa_num_configured_cpus() / numa_num_configured_nodes();

    const std::string& ip = parser.takeParseArg<std::string>("-ip", "[Info] No IP given. Pass a server IP with [-ip]. Default: 127.0.0.1", "127.0.0.1", false);
    const size_t port = parser.takeParseArg<size_t>("-port", "[Info] No Port given. Pass a port with [-port]. Default: 23232", 23232, false);

    const std::string& prettyName = parser.takeParseArg<std::string>("-name", "[Info] No pretty name given. Set a name with [-name]. Default: [ComputeUnit].", "ComputeUnit", false);
    const size_t worker = parser.takeParseArg<size_t>("-worker", "[Info] No Worker limit given. Set an initial worker count with [-worker]. Using default: [Logical cores per node].", cpus_per_node, false);
    const std::string& pathToData = parser.takeParseArg<std::string>("-path", "[Info] No path to data given. Set with [-path].", "", false);
    const std::string load_binary = parser.takeParseArg<std::string>("-basedata", "[Info] Loading SSB as CSV. Set with [-basedata] to \'bin\' or \'csv\'", "bin", false);
    const ssize_t node = parser.takeParseArg<ssize_t>("-node", "[Info] No NUMA node affinity given. Set with [-node]. -1 means all nodes. Using default: [-1].", -1, false);
    const ssize_t cxl_node = parser.takeParseArg<ssize_t>("-cxl_node", "[Info] No CXL-NUMA node affinity given. Set with [-cxl_node]. -1 means any node is used for data. Using default: [-1].", -1, false);

    const std::string& monitor_ip = parser.takeParseArg<std::string>("-monitor_ip", "[Info] No minotor IP given. Pass a server IP with [-monitor_ip], unit is starting without monitoring!", "", false);
    const size_t monitor_port = parser.takeParseArg<size_t>("-monitor_port", "[Info] No monitor Port given. Pass a port with [-monitor_port], unit is starting without monitoring!", 0, false);
    const size_t monitor_interval = parser.takeParseArg<size_t>("-monitor_interval", "[Info] No monitor interval given. Standard interval of 100 millisecond will be used.", 100, false);
    const size_t monitor_acc = parser.takeParseArg<size_t>("-monitor_acc", "[Info] No accumulation count - Slim Mode disabled. Client will send each batch explicitly.", 1, false);

    if (pathToData.size() > 0) {
        LOG_DEBUG1("[ComputeUnit] Ingesting SSB data from path: " << pathToData << " to NUMA node " << cxl_node << std::endl;)
        datareader::ingestSSBData(pathToData, load_binary, cxl_node);
        LOG_INFO("[ComputeUnit] Data ingested!" << std::endl;)
        VLOG("This is a VLOG test!");
    }

    // Set CPU usage to the local NUMA node
    if (node != -1) {
        if (numa_run_on_node(node) != 0) {
            std::perror("numa_run_on_node");
            return EXIT_FAILURE;
        }
    }

    std::mutex groupToPlanMutex;

    std::unordered_map<uint64_t, std::vector<uint32_t>> groupToPlanMapping;
    std::unordered_map<uint32_t, uint64_t> planToGroupMapping;
    std::unordered_map<uint64_t, std::unordered_set<std::string>> groupToTableMapping;
    std::unordered_map<uint32_t, std::vector<ExtendedResult>> planToExtendedResults;

    tuddbs::TCPClient client(ip, port);

    // Configure orchestrator
    orchestrator::PlanOrchestrator::Config config;
    config.gcInterval = std::chrono::milliseconds(1000);
    config.maxPendingCleanup = 10;

    // Create orchestrator
    orchestrator::PlanOrchestrator orch(config);

    auto onFinish = [&client, &orch, &groupToPlanMutex, &groupToPlanMapping, &planToGroupMapping, &groupToTableMapping, &planToExtendedResults](const Task& task) -> void {
        auto sendPlanResponse = [&client, &task](PlanResponse response) -> void {
            tuddbs::TCPMetaInfo info;
            info.package_type = TcpPackageType::PLAN_RESPONSE;
            info.payload_size = response.ByteSizeLong();
            info.src_uuid = client.getUuid();
            info.tgt_uuid = task.response_target;
            void* out_mem = malloc(info.bytesize());
            if (!out_mem) {
                std::cerr << "[ComputeUnit::onFinish] Wasn't able to allocate the requested memory!" << std::endl;
                exit(-1);
            }
            const size_t message_size = tuddbs::Utility::serializeItemToMemory(out_mem, response, info);
            client.notifyHost(out_mem, info.bytesize());
            free(out_mem);
        };

        if (task.response.planid() == 0 && task.response.itemid() == 0) {
            return;
        }

        LOG_DEBUG2("Returning response:" << task.response.DebugString() << "\n======" << std::endl;)

        if (task.response.success()) {
            LOG_DEBUG1("Task completed successfully." << std::endl;)
            orch.onItemCompleted(task.original_work_item.planid(), task.original_work_item.itemid());

            if (task.response.has_extendedresult()) {
                const auto& ext = task.response.extendedresult();
                planToExtendedResults[task.original_work_item.planid()].emplace_back(std::move(ext));
            }

            if (task.original_work_item.operatorid() == OperatorType::OP_RESULT) {
                LOG_DEBUG1("Plan " << task.original_work_item.planid() << " completed!" << std::endl;)

                orch.finalizePlan(task.original_work_item.planid());

                PlanResponse response;
                response.set_planid(task.original_work_item.planid());
                response.set_success(true);
                response.set_info("Plan completed successfully.");

                if (planToExtendedResults.contains(task.original_work_item.planid())) {
                    auto* extended_results = response.mutable_extendedresults();
                    extended_results->Reserve(planToExtendedResults[task.original_work_item.planid()].size());
                    for (const auto& ext : planToExtendedResults[task.original_work_item.planid()]) {
                        *extended_results->Add() = std::move(ext);
                    }
                }

                sendPlanResponse(response);
                const uint32_t planId = task.original_work_item.planid();
                {
                    std::lock_guard<std::mutex> lock(groupToPlanMutex);
                    const uint64_t groupId = planToGroupMapping[planId];

                    if (groupId > 0) {
                        if (groupToPlanMapping[groupId].size() == 1) {
                            for (const std::string& table : groupToTableMapping[groupId]) {
                                DataCatalog::getInstance().dropTable(table);
                            }
                            groupToTableMapping.erase(groupId);
                            groupToPlanMapping.erase(groupId);
                        } else if (groupToPlanMapping[groupId].size() == 0) {
                            LOG_ERROR("[onFinish] No entry in groupToPlanMapping for groupId " << groupId << std::endl;)
                        } else {
                            auto it = groupToPlanMapping.find(groupId);
                            if (it != groupToPlanMapping.end()) {
                                std::erase(it->second, planId);
                            }
                        }
                        planToGroupMapping.erase(planId);
                    }
                }

                DataCatalog::getInstance().dropTable(task.original_work_item.resultdata().resultcolumns().Get(0).tabname());
            }
        } else {
            LOG_DEBUG1("Task FAILED!" << std::endl;)
            orch.onItemFailed(task.original_work_item.planid(), task.original_work_item.itemid());

            std::string resp_info = std::format("Plan failed at item: {}\nwith message:\n{}", task.original_work_item.itemid(), task.response.info());

            PlanResponse response;
            response.set_planid(task.original_work_item.planid());
            response.set_success(false);
            response.set_info(resp_info);

            sendPlanResponse(response);
        }
    };

    auto onForward = [&client](const Task& task) -> void {
        LOG_CONSOLE("I am now passive and must forward this Task:" << std::endl;)
        ForwardedWorkRequest forwarded_request;
        *forwarded_request.mutable_request()->mutable_workitem() = task.original_work_item;

        tuddbs::TCPMetaInfo info;
        info.package_type = TcpPackageType::REROUTE_WORK;
        info.payload_size = forwarded_request.ByteSizeLong();
        info.src_uuid = client.getUuid();
        info.tgt_uuid = task.response_target;
        void* out_mem = malloc(sizeof(tuddbs::TCPMetaInfo) + info.payload_size);
        if (!out_mem) {
            std::cerr << "[ComputeUnit::onForward] Wasn't able to allocate the requested memory!" << std::endl;
            exit(-1);
        }
        const size_t message_size = tuddbs::Utility::serializeItemToMemory(out_mem, forwarded_request, info);
        client.notifyHost(out_mem, info.bytesize());
        free(out_mem);
    };

    SupportedOperators opFlags(SupportedOperators::SupportedClass::FilterOperations,
                               SupportedOperators::SupportedClass::SetOperations,
                               SupportedOperators::SupportedClass::MaterializeOperations,
                               SupportedOperators::SupportedClass::FetchOperations,
                               SupportedOperators::SupportedClass::JoinOperations,
                               SupportedOperators::SupportedClass::AggregateOperations,
                               SupportedOperators::SupportedClass::SortOperations,
                               SupportedOperators::SupportedClass::GroupOperations,
                               SupportedOperators::SupportedClass::MapOperations,
                               SupportedOperators::SupportedClass::TestDataGeneration,
                               SupportedOperators::SupportedClass::DataTransfer);

    WorkerPool pool(worker, node);
    pool.setOnFinish(onFinish);
    pool.setOnForward(onForward);
    OperatorDispatcher dispatcher(&pool, opFlags);

    orch.setDispatcher(&dispatcher);

    auto updateUnitInfo_cb = [&client, prettyName, worker](tuddbs::TCPMetaInfo* meta, void* data, size_t len) -> void {
        using namespace tuddbs;
        LOG_DEBUG1("[UpdateUnitInfo Callback] Invoked." << std::endl;)
        UnitDefinition unit;
        unit.set_unit_type(static_cast<uint32_t>(UnitType::COMPUTE_UNIT));
        unit.set_prettyname(prettyName);

        TCPMetaInfo info;
        info.package_type = TcpPackageType::UPDATE_UNIT_TYPE;
        info.payload_size = unit.ByteSizeLong();
        info.src_uuid = client.getUuid();
        void* out_mem = malloc(sizeof(TCPMetaInfo) + info.payload_size);
        if (!out_mem) {
            std::cerr << "[ComputeUnit::updateUnitInfo_cb] Wasn't able to allocate the requested memory!" << std::endl;
            exit(-1);
        }

        const size_t message_size = tuddbs::Utility::serializeItemToMemory(out_mem, unit, info);

        client.notifyHost(out_mem, message_size);
        free(out_mem);

        send_worker_count(&client, worker);
    };

    auto text_cb = [&client](tuddbs::TCPMetaInfo* meta, void* data, size_t len) -> void {
        std::string str(reinterpret_cast<char*>(data), len);
        LOG_DEBUG1("Text Received: " << str << std::endl;)
    };

    auto work_cb = [&client, &dispatcher](tuddbs::TCPMetaInfo* meta, void* data, size_t len) -> void {
        WorkRequest request;
        switch (meta->package_type) {
            case TcpPackageType::WORK: {
                LOG_DEBUG1("Received a DIRECT WorkRequest." << std::endl;)
                request.ParseFromArray(data, len);
            }; break;
            case TcpPackageType::REROUTE_WORK: {
                LOG_DEBUG1("Received a FORWARDED WorkRequest." << std::endl;)
                ForwardedWorkRequest fwd;
                fwd.ParseFromArray(data, len);
                request.CopyFrom(fwd.request());
            }; break;
            default: {
                LOG_WARNING("Work Callback without suitable work!" << std::endl;)
                return;
            };
        }
        dispatcher.dispatch(meta->src_uuid, request.workitem());
    };

    auto configAction_cb = [&](tuddbs::TCPMetaInfo* meta, void* data, size_t len) -> void {
        ddd::config::ConfigurationItem ci;
        ci.ParseFromArray(data, len);

        LOG_DEBUG1("I received a Config Action from uuid<" << meta->src_uuid << ">" << std::endl;)
        // ci.PrintDebugString();

        std::string response;
        if (ci.type() == ddd::config::SET_WORKER) {
            LOG_DEBUG1("I should update my worker count to " << ci.worker_count() << std::endl;)
            response = "[" + prettyName + "] Workers updated to " + std::to_string(ci.worker_count());
#ifdef SINGLE_CPU_EXEC
            client.textResponse("I am a Single Core Executor.", 0);
#else
            pool.update_workers(ci.worker_count());
            send_worker_count(&client, pool.get_active_workers());
#endif
        } else if (ci.type() == ddd::config::RESET_CATALOG) {
            LOG_INFO("[ComputeUnit ] I am clearing my DataCatalog." << std::endl;)
            DataCatalog::getInstance().clearCatalog(false);

            response = "[" + prettyName + "] Catalog cleared.";
        } else {
            LOG_ERROR("I received a strange configuration action.";)
        }

        client.textResponse(response, 0);
        client.textResponse(response, meta->src_uuid);
    };

    auto query_plan_cb = [&client, &dispatcher, &orch](tuddbs::TCPMetaInfo* meta, void* data, size_t len) -> void {
        WorkRequest request;
        request.ParseFromArray(data, len);

        LOG_DEBUG1("Received a QueryPlan." << std::endl;)

        QueryPlan plan = request.queryplan();

        if (!orch.submitPlan(plan, meta->src_uuid)) {
            PlanResponse response;
            response.set_planid(plan.planid());
            response.set_info("[ERROR] The PlanOrchestrator is not running and the plan couldn't be submitted!");
            response.set_success(false);
        }
    };

    auto query_group_cb = [&client, &dispatcher, &orch, &groupToPlanMutex, &groupToPlanMapping, &planToGroupMapping, &groupToTableMapping](tuddbs::TCPMetaInfo* meta, void* data, size_t len) -> void {
        WorkRequest request;
        request.ParseFromArray(data, len);
        QueryGroup group = request.querygroup();

        LOG_INFO("Received a QueryGroup with id: " << group.groupid() << std::endl;)

        for (WorkItem witem : group.columntransfers()) {
            DataTransferItem transfer = witem.transferdata();

            auto column = DataCatalog::getInstance().getColumnByName(transfer.source().tabname(), transfer.source().colname());

            std::shared_ptr<Column> resColumn = std::make_shared<Column>(0, column->datatype, column->sizeInBytes, column->elements, false, false, -1);
            DataCatalog::getInstance().addColumn(transfer.destination().tabname(), transfer.destination().colname(), resColumn);

            dispatcher.dispatch(meta->src_uuid, witem);

            {
                std::lock_guard<std::mutex> lock(groupToPlanMutex);
                groupToTableMapping[group.groupid()].emplace(transfer.destination().tabname());
            }
        }

        for (QueryPlan plan : group.plans()) {
            {
                std::lock_guard<std::mutex> lock(groupToPlanMutex);
                groupToPlanMapping[group.groupid()].push_back(plan.planid());
                planToGroupMapping.emplace(plan.planid(), group.groupid());
            }

            if (!orch.submitPlan(plan, meta->src_uuid)) {
                PlanResponse response;
                response.set_planid(plan.planid());
                response.set_info("[ERROR] The PlanOrchestrator is not running and the plan couldn't be submitted!");
                response.set_success(false);
            }
        }
    };

    client.addCallback(TcpPackageType::UPDATE_UNIT_TYPE, updateUnitInfo_cb);
    client.addCallback(TcpPackageType::WORK, work_cb);
    client.addCallback(TcpPackageType::QUERY_PLAN, query_plan_cb);
    client.addCallback(TcpPackageType::QUERY_GROUP, query_group_cb);
    client.addCallback(TcpPackageType::REROUTE_WORK, work_cb);
    client.addCallback(TcpPackageType::TEXT, text_cb);
    client.addCallback(TcpPackageType::CONFIGURATION_ACTION, configAction_cb);

    client.start();

    // necessary for initializing if not already done
    DataCatalog::getInstance();

    std::optional<MonitorClient> monitor_client;
    if (!(monitor_ip=="" || monitor_port==0)) {
        AppMetricCallbacks callbacks;
        callbacks.get_intermediate_table_size = []() -> size_t {
            return DataCatalog::getInstance().getTableSize("intermediate");
        };
        callbacks.get_column_count = []() -> size_t {
            return DataCatalog::getInstance().getColumnCount();
        };
        callbacks.get_total_catalog_size = []() -> size_t {
            return DataCatalog::getInstance().getTotalCatalogSize();
        };

        monitor_client.emplace(
            monitor_ip,
            monitor_port,
            client.getUuid(),
            UnitType::COMPUTE_UNIT,
            monitor_interval,
            monitor_acc,
            callbacks
        );
    }

    client.waitUntilChannelClosed();

    return 0;
}
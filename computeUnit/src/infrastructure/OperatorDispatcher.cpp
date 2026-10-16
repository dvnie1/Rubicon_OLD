#include <UnitDefinition.pb.h>
#include <WorkResponse.pb.h>

#include <Utility.hpp>
#include <bitset>
#include <factories/AggregateFactory.hpp>
#include <factories/DataTransferFactory.hpp>
#include <factories/FetchDataFactory.hpp>
#include <factories/FilterFactory.hpp>
#include <factories/GroupFactory.hpp>
#include <factories/JoinFactory.hpp>
#include <factories/MapFactory.hpp>
#include <factories/MaterializeFactory.hpp>
#include <factories/ResultFactory.hpp>
#include <factories/SetOperatorFactory.hpp>
#include <factories/SortFactory.hpp>
#include <factories/TestDataFactory.hpp>
#include <infrastructure/OperatorDispatcher.hpp>

#ifdef SINGLE_CPU_EXEC
OperatorDispatcher::OperatorDispatcher(const SupportedOperators& flags) {
#else
OperatorDispatcher::OperatorDispatcher(WorkerPool* pool, const SupportedOperators& flags) : m_pool{pool} {
#endif
    test_and_add<FilterFactory>(flags, SupportedOperators::SupportedClass::FilterOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kFilterData));
    test_and_add<SetOperatorFactory>(flags, SupportedOperators::SupportedClass::SetOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kSetData));
    test_and_add<MaterializeFactory>(flags, SupportedOperators::SupportedClass::MaterializeOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kMaterializeData));
    test_and_add<FetchDataFactory>(flags, SupportedOperators::SupportedClass::FetchOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kFetchData));
    test_and_add<ResultFactory>(flags, SupportedOperators::SupportedClass::FetchOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kResultData));
    test_and_add<AggregateFactory>(flags, SupportedOperators::SupportedClass::AggregateOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kAggData));
    test_and_add<JoinFactory>(flags, SupportedOperators::SupportedClass::JoinOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kJoinData));
    test_and_add<SortFactory>(flags, SupportedOperators::SupportedClass::SortOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kSortData));
    test_and_add<MapFactory>(flags, SupportedOperators::SupportedClass::MapOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kMapData));
    test_and_add<DataTransferFactory>(flags, SupportedOperators::SupportedClass::DataTransfer, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kTransferData));
    // test_and_add<GroupFactory>(flags, SupportedOperators::SupportedClass::GroupOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kGroupData));

    test_and_add<GroupFactory>(flags, SupportedOperators::SupportedClass::GroupOperations, computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, WorkItem::OpDataCase::kMultiGroupData));

    test_and_add<TestDataFactory>(flags, SupportedOperators::SupportedClass::TestDataGeneration, computeIdFromEnum(WorkRequest::RequestItemCase::kTestDataSetup, 0));

}

OperatorDispatcher::RequestOperatorId OperatorDispatcher::computeIdFromEnum(const RequestIdPart reqId, const OperatorIdPart opId) const {
    const size_t shift_distance = sizeof(RequestIdPart) * CHAR_BIT;
    return (static_cast<RequestOperatorId>(reqId) << shift_distance) | static_cast<RequestOperatorId>(opId);
}

OperatorDispatcher::RequestOperatorId OperatorDispatcher::getIdForWorkRequest(const WorkRequest& request) const {
    RequestOperatorId id = 0;
    const RequestIdPart reqId = request.requestItem_case();
    OperatorIdPart opId = 0;

    switch (request.requestItem_case()) {
        case WorkRequest::RequestItemCase::kQueryPlan: {
            LOG_INFO("[OperatorDispatcher] At the time of writing, QueryPlans had no switchable component. Adapt this implementation if necessary." << std::endl;)
            opId = static_cast<uint16_t>(0);
        } break;
        case WorkRequest::RequestItemCase::kTestDataSetup: {
            opId = static_cast<uint16_t>(0);
        } break;
        case WorkRequest::RequestItemCase::kWorkItem: {
            opId = static_cast<uint16_t>(request.workitem().opData_case());
        } break;
        default: {
            opId = 0;
        }
    }

    return computeIdFromEnum(reqId, opId);
}

OperatorDispatcher::~OperatorDispatcher() {
}

#ifdef SINGLE_CPU_EXEC
Task OperatorDispatcher::dispatch(const uint64_t target_uuid, WorkItem witem) const {
#else
void OperatorDispatcher::dispatch(const uint64_t target_uuid, WorkItem witem) const {
#endif

    Task t(std::unique_ptr<AbstractOperator>(), target_uuid);
    t.original_work_item = witem;

    const RequestOperatorId reqOpId = computeIdFromEnum(WorkRequest::RequestItemCase::kWorkItem, static_cast<uint16_t>(witem.opData_case()));
#ifdef SINGLE_CPU_EXEC
    if (m_factories.contains(reqOpId)) {
        AbstractOperator* op = m_factories.at(reqOpId)->create_operator(witem);
        t.response = std::move(op->run());
    } else {
        LOG_ERROR("[OperatorDispatcher] Operator not supported:\n"
                      << witem.DebugString() << std::endl;)
        t.response.set_info("[OperatorDispatcher] Requested Operation is unknown to me.");
        t.response.set_success(false);
    }
    return t;
#else
    if (m_factories.contains(reqOpId)) {
        AbstractOperator* op = m_factories.at(reqOpId)->create_operator(witem);
        t.op = std::unique_ptr<AbstractOperator>(op);
        m_pool->enqueue_operator(t);
    } else {
        LOG_ERROR("[OperatorDispatcher] Operator " << reqOpId << " not supported:\n" << witem.DebugString() << std::endl;)
        t.response.set_info("[OperatorDispatcher] Requested Operation is unknown to me: " + std::to_string(reqOpId));
        t.response.set_success(false);
        m_pool->finalizeTask(t);
    }
#endif
}

void OperatorDispatcher::add_factory(RequestOperatorId id, AbstractOperatorFactory* factory) {
    if (m_factories.contains(id)) {
        LOG_ERROR("[OperatorDispatcher] Error - Adding a new factory for ident " << static_cast<size_t>(id) << std::endl;)
        exit(-10);
    }
    m_factories[id] = factory;
}
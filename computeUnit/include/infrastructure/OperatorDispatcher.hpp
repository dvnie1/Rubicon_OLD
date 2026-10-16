#pragma once

#include <WorkRequest.pb.h>
#include <WorkResponse.pb.h>

#include <Logger.hpp>
#include <factories/AbstractOperatorFactory.hpp>
#include <functional>
#include <infrastructure/SupportedOperators.hpp>
#include <infrastructure/WorkerPool.hpp>
#include <unordered_map>

/**
 * @brief Responsible for mapping operator codes from WorkItems to actual operator wrappers and executing or submitting the created operator.
 *
 */
class OperatorDispatcher {
   public:
    /*
        We create the RequestOperatorId part as composite of WorkRequest.requestItem_case and the operator ID of the contained message.
        Make sure that the size of both sub-parts adds up to the data type of RequestOperatorId.
    */
    typedef uint32_t RequestOperatorId;
    typedef uint16_t RequestIdPart;
    typedef uint16_t OperatorIdPart;

#ifdef SINGLE_CPU_EXEC
    OperatorDispatcher(const SupportedOperators& flags);
#else
    OperatorDispatcher(WorkerPool* pool, const SupportedOperators& flags);
#endif
    ~OperatorDispatcher();

#ifdef SINGLE_CPU_EXEC
    Task dispatch(const uint64_t target_uuid, WorkItem witem) const;
#else
    void dispatch(const uint64_t target_uuid, WorkItem witem) const;
#endif

    void add_factory(RequestOperatorId id, AbstractOperatorFactory* factory);
    RequestOperatorId computeIdFromEnum(const RequestIdPart reqId, const OperatorIdPart opId) const;
    RequestOperatorId getIdForWorkRequest(const WorkRequest& request) const;

   private:
    std::unordered_map<RequestOperatorId, AbstractOperatorFactory*> m_factories;
#ifndef SINGLE_CPU_EXEC
    WorkerPool* m_pool;
#endif

    template <class FactoryT>
    void test_and_add(const SupportedOperators& flags, SupportedOperators::SupportedClass flag, RequestOperatorId compositeOpId) {
        if (flags.isSet(flag)) {
            LOG_DEBUG1("[OperatorDispatcher] Added Factory for flag " << SupportedOperators::supportedClassToString(flag) << " (ID is " << compositeOpId << ")" << std::endl;)
            add_factory(compositeOpId, new FactoryT());
        }
    }
};
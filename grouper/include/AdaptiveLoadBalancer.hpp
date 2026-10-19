#pragma once

#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <memory>
#include <shared_mutex>

#include "MetricsClient.hpp"
#include "XgbEstimator.hpp"
#include "ThompsonSampler.hpp"
#include "PlanDAG.hpp"

namespace lb {
    struct PendingGroup {
        uint64_t selected_cu;
        std::vector<float> query_features;
        std::vector<float> bandit_context;
        ExecutionMetrics prediction;
        std::chrono::steady_clock::time_point start_time;
        size_t remaining_plans;
        bool all_succeeded = true;
    };

    class AdaptiveLoadBalancer {
    public:
        AdaptiveLoadBalancer(const std::string &vm_url, const std::string &latency_model = "",
                             const std::string &ram_model = "");

        ~AdaptiveLoadBalancer();

        uint64_t assignGroup(uint64_t group_id,
                             const std::vector<const plan::PlanDAG *> &dags,
                             const std::vector<uint64_t> &available_cus,
                             const std::unordered_map<std::string, uint64_t> &schema);

        void onPlanCompleted(uint32_t plan_id, bool success);

    private:
        void finalizeGroup(const PendingGroup &group_info, double actual_latency_ms);

        static constexpr size_t BANDIT_FEATURES = 4;

        std::unique_ptr<MetricsClient> metrics_client_;
        std::unique_ptr<XgbEstimator> xgb_;
        std::unique_ptr<ThompsonSampler> sampler_;

        mutable std::shared_mutex tracking_mutex_;
        std::unordered_map<uint64_t, PendingGroup> pending_groups_;
        std::unordered_map<uint32_t, uint64_t> plan_to_group_;
    };
}

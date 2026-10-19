#pragma once

#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <atomic>
#include <chrono>
#include <fstream>

#include "MetricsClient.hpp"
#include "XgbEstimator.hpp"
#include "ThompsonSampler.hpp"
#include "PlanDAG.hpp"

namespace lb {

struct LoadBalancerConfig {
    std::string vm_url = "http://127.0.0.1:8428";
    std::string latency_model_path = "";
    std::string ram_model_path = "";
    std::string log_csv_path = "lb_execution_log.csv";
    bool enable_warmup = true;
    size_t warmup_groups = 100;
    size_t retrain_interval = 50;
    size_t num_trees = 200;
};

struct PendingGroup {
    uint64_t group_id;
    uint64_t selected_cu;
    std::vector<float> query_features;
    std::vector<float> bandit_context;
    ExecutionMetrics prediction;
    std::chrono::steady_clock::time_point start_steady;
    int64_t start_sys_ms;
    size_t remaining_plans;
    bool was_warmup;
    bool all_succeeded = true;
};

class AdaptiveLoadBalancer {
public:
    explicit AdaptiveLoadBalancer(LoadBalancerConfig config);
    ~AdaptiveLoadBalancer();

    uint64_t assignGroup(uint64_t group_id,
                         const std::vector<const plan::PlanDAG*>& dags,
                         const std::vector<uint64_t>& available_cus,
                         const std::unordered_map<std::string, uint64_t>& schema = {});

    void onPlanCompleted(uint32_t plan_id, bool success);

private:
    void finalizeGroup(const PendingGroup& group_info, double actual_latency_ms, int64_t end_sys_ms);
    void logGroupExecution(const PendingGroup& group_info, const ExecutionMetrics& actuals);

    static constexpr size_t BANDIT_FEATURES = 4;

    LoadBalancerConfig config_;
    std::unique_ptr<MetricsClient> metrics_client_;
    std::unique_ptr<XgbEstimator> xgb_;
    std::unique_ptr<ThompsonSampler> sampler_;

    std::atomic<bool> is_ready_{false};
    std::atomic<size_t> rr_counter_{0};

    mutable std::shared_mutex tracking_mutex_;
    std::unordered_map<uint64_t, PendingGroup> pending_groups_;
    std::unordered_map<uint32_t, uint64_t> plan_to_group_;

    std::mutex dataset_mutex_;
    std::vector<float> history_features_;
    std::vector<float> history_latencies_;
    std::vector<float> history_rams_;
    size_t completed_groups_{0};

    std::mutex log_mutex_;
    std::ofstream log_file_;
};

}
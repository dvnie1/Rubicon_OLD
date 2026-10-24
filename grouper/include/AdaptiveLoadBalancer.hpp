#pragma once

#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>
#include <fstream>

#include "MetricsClient.hpp"
#include "XgbEstimator.hpp"
#include "ThompsonSampler.hpp"
#include "QueryVectorizer.hpp"
#include "PlanDAG.hpp"

namespace lb {

struct LoadBalancerConfig {
    std::string vm_url = "http://127.0.0.1:8428";
    std::string latency_model_path = "";
    std::string ram_model_path = "";
    std::string log_csv_path = "lb_execution_log.csv";
    bool enable_warmup = true;
    bool filter_overlapped_for_xgb = true;
    size_t warmup_groups = 100;
    size_t retrain_interval = 50;
    size_t num_trees = 100;
};

struct PendingGroup {
    uint64_t group_id;
    uint64_t selected_cu;
    std::vector<float> query_features;
    std::vector<float> bandit_context;
    ExecutionMetrics prediction;
    double baseline_ram_bytes;
    std::chrono::steady_clock::time_point start_steady;
    int64_t start_sys_ms;
    size_t remaining_plans;
    size_t max_concurrent_on_cu;
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
    void onWorkerCount(uint64_t uuid, size_t workers);

private:
    void finalizeGroup(const PendingGroup& group_info, double actual_latency_ms, int64_t end_sys_ms);
    void logGroupExecution(const PendingGroup& group_info, const ExecutionMetrics& actuals);
    void launchAsyncRetrain(std::vector<float> features, std::vector<float> latencies, std::vector<float> rams, size_t count, bool is_warmup_finish);

    static constexpr size_t XGB_FEATURES = QueryVectorizer::GROUP_VECTOR_SIZE + 1;
    static constexpr size_t BANDIT_FEATURES = 6;

    LoadBalancerConfig config_;
    std::unique_ptr<MetricsClient> metrics_client_;
    std::unique_ptr<XgbEstimator> xgb_;
    std::unique_ptr<ThompsonSampler> sampler_;

    std::atomic<bool> is_ready_{false};
    std::atomic<bool> is_retraining_{false};
    std::atomic<size_t> rr_counter_{0};
    std::thread retrain_thread_;

    mutable std::shared_mutex tracking_mutex_;
    std::unordered_map<uint64_t, PendingGroup> pending_groups_;
    std::unordered_map<uint32_t, uint64_t> plan_to_group_;
    std::unordered_map<uint64_t, size_t> cu_in_flight_;
    std::unordered_map<uint64_t, size_t> cu_workers_;
    std::unordered_map<uint64_t, double> cu_reserved_ram_;
    std::unordered_map<uint64_t, double> cu_reserved_lat_;

    std::mutex dataset_mutex_;
    std::vector<float> history_features_;
    std::vector<float> history_latencies_;
    std::vector<float> history_rams_;
    size_t completed_groups_{0};

    std::mutex log_mutex_;
    std::ofstream log_file_;
};

} // namespace lb
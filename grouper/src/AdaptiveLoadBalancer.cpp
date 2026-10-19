#include "AdaptiveLoadBalancer.hpp"
#include "QueryVectorizer.hpp"
#include <algorithm>
#include <filesystem>
#include <iostream>

namespace lb {

static int64_t currentEpochMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

AdaptiveLoadBalancer::AdaptiveLoadBalancer(LoadBalancerConfig config)
    : config_(std::move(config)) {

    metrics_client_ = std::make_unique<MetricsClient>(config_.vm_url);
    metrics_client_->startPolling(1000);

    xgb_ = std::make_unique<XgbEstimator>(
        config_.latency_model_path,
        config_.ram_model_path,
        QueryVectorizer::GROUP_VECTOR_SIZE,
        config_.num_trees
    );
    sampler_ = std::make_unique<ThompsonSampler>(BANDIT_FEATURES, 1.0);

    is_ready_ = !config_.enable_warmup || xgb_->hasLoadedModels();

    if (!config_.log_csv_path.empty()) {
        bool exists = std::filesystem::exists(config_.log_csv_path);
        log_file_.open(config_.log_csv_path, std::ios::app);
        if (log_file_.is_open() && !exists) {
            log_file_ << "timestamp_ms,group_id,selected_cu,is_warmup,pred_latency_ms,actual_latency_ms,pred_ram_bytes,actual_ram_bytes";
            for (size_t i = 0; i < QueryVectorizer::GROUP_VECTOR_SIZE; ++i) {
                log_file_ << ",f_" << i;
            }
            log_file_ << "\n";
            log_file_.flush();
        }
    }
}

AdaptiveLoadBalancer::~AdaptiveLoadBalancer() {
    metrics_client_->stopPolling();
    if (!config_.latency_model_path.empty() || !config_.ram_model_path.empty()) {
        xgb_->saveModels(config_.latency_model_path, config_.ram_model_path);
    }
}

uint64_t AdaptiveLoadBalancer::assignGroup(uint64_t group_id,
                                           const std::vector<const plan::PlanDAG*>& dags,
                                           const std::vector<uint64_t>& available_cus,
                                           const std::unordered_map<std::string, uint64_t>& schema) {
    if (available_cus.empty()) return 0;

    std::vector<float> query_features = QueryVectorizer::vectorizeGroup(dags, schema);
    bool ready = is_ready_.load(std::memory_order_acquire);

    ExecutionMetrics prediction{0.0, 0.0};
    uint64_t selected_cu = 0;
    std::vector<float> selected_context(BANDIT_FEATURES, 0.0f);

    if (!ready) {
        size_t idx = rr_counter_.fetch_add(1, std::memory_order_relaxed) % available_cus.size();
        selected_cu = available_cus[idx];
    } else {
        prediction = xgb_->predict(query_features);
        auto cu_states = metrics_client_->getCurrentStates();

        std::unordered_map<uint64_t, std::vector<float>> cu_bandit_contexts;
        for (uint64_t cu : available_cus) {
            float cpu = 0.0f;
            float ram = 0.0f;
            if (cu_states.contains(cu)) {
                cpu = static_cast<float>(cu_states[cu].cpu_usage_percent);
                ram = static_cast<float>(cu_states[cu].ram_available_bytes);
            }
            cu_bandit_contexts[cu] = {
                static_cast<float>(prediction.latency_ms),
                static_cast<float>(prediction.ram_peak_bytes),
                cpu,
                ram
            };
        }

        selected_cu = sampler_->selectTarget(available_cus, cu_bandit_contexts);
        selected_context = std::move(cu_bandit_contexts[selected_cu]);
    }

    {
        std::unique_lock lock(tracking_mutex_);
        pending_groups_[group_id] = PendingGroup{
            group_id,
            selected_cu,
            std::move(query_features),
            std::move(selected_context),
            prediction,
            std::chrono::steady_clock::now(),
            currentEpochMs(),
            dags.size(),
            !ready,
            true
        };
        for (const auto* dag : dags) {
            plan_to_group_[dag->getPlanId()] = group_id;
        }
    }

    return selected_cu;
}

void AdaptiveLoadBalancer::onPlanCompleted(uint32_t plan_id, bool success) {
    PendingGroup finished_group;
    bool group_done = false;
    double actual_latency_ms = 0.0;
    int64_t end_sys_ms = 0;

    {
        std::unique_lock lock(tracking_mutex_);
        auto map_it = plan_to_group_.find(plan_id);
        if (map_it == plan_to_group_.end()) return;

        uint64_t group_id = map_it->second;
        plan_to_group_.erase(map_it);

        auto grp_it = pending_groups_.find(group_id);
        if (grp_it == pending_groups_.end()) return;

        if (!success) grp_it->second.all_succeeded = false;

        if (--grp_it->second.remaining_plans == 0) {
            auto now_steady = std::chrono::steady_clock::now();
            end_sys_ms = currentEpochMs();
            actual_latency_ms = std::chrono::duration<double, std::milli>(now_steady - grp_it->second.start_steady).count();
            finished_group = std::move(grp_it->second);
            pending_groups_.erase(grp_it);
            group_done = true;
        }
    }

    if (group_done) {
        finalizeGroup(finished_group, actual_latency_ms, end_sys_ms);
    }
}

void AdaptiveLoadBalancer::finalizeGroup(const PendingGroup& group_info, double actual_latency_ms, int64_t end_sys_ms) {
    if (!group_info.all_succeeded) {
        if (!group_info.was_warmup) {
            sampler_->updateDistribution(group_info.bandit_context, 0.0);
        }
        return;
    }

    double actual_ram_bytes = metrics_client_->getPeakRamInWindow(
        group_info.selected_cu,
        group_info.start_sys_ms,
        end_sys_ms
    );

    ExecutionMetrics actuals{actual_latency_ms, actual_ram_bytes};
    logGroupExecution(group_info, actuals);

    if (!group_info.was_warmup) {
        double predicted_lat = std::max(group_info.prediction.latency_ms, 1.0);
        double actual_lat = std::max(actual_latency_ms, 1.0);
        double reward = std::clamp(predicted_lat / actual_lat, 0.0, 1.0);
        sampler_->updateDistribution(group_info.bandit_context, reward);
    }

    std::vector<float> snapshot_features;
    std::vector<float> snapshot_latencies;
    std::vector<float> snapshot_rams;
    size_t sample_count = 0;
    bool trigger_retrain = false;
    bool finished_warmup = false;

    {
        std::lock_guard lock(dataset_mutex_);
        history_features_.insert(history_features_.end(), group_info.query_features.begin(), group_info.query_features.end());
        history_latencies_.push_back(static_cast<float>(actual_latency_ms));
        history_rams_.push_back(static_cast<float>(actual_ram_bytes));
        completed_groups_++;

        if (!is_ready_.load(std::memory_order_relaxed)) {
            if (completed_groups_ >= config_.warmup_groups) {
                trigger_retrain = true;
                finished_warmup = true;
            }
        } else if (config_.retrain_interval > 0 &&
                   (completed_groups_ - config_.warmup_groups) % config_.retrain_interval == 0) {
            trigger_retrain = true;
        }

        if (trigger_retrain) {
            snapshot_features = history_features_;
            snapshot_latencies = history_latencies_;
            snapshot_rams = history_rams_;
            sample_count = completed_groups_;
        }
    }

    if (trigger_retrain) {
        xgb_->retrainModels(snapshot_features, snapshot_latencies, snapshot_rams, sample_count);
        if (finished_warmup) {
            is_ready_.store(true, std::memory_order_release);
            std::cout << "[AdaptiveLoadBalancer] Warm-up completed (" << sample_count << " groups). Estimators active.\n";
        }
    }
}

void AdaptiveLoadBalancer::logGroupExecution(const PendingGroup& group_info, const ExecutionMetrics& actuals) {
    if (!log_file_.is_open()) return;

    std::lock_guard lock(log_mutex_);
    log_file_ << group_info.start_sys_ms << ","
              << group_info.group_id << ","
              << group_info.selected_cu << ","
              << (group_info.was_warmup ? 1 : 0) << ","
              << group_info.prediction.latency_ms << ","
              << actuals.latency_ms << ","
              << group_info.prediction.ram_peak_bytes << ","
              << actuals.ram_peak_bytes;

    for (float f : group_info.query_features) {
        log_file_ << "," << f;
    }
    log_file_ << "\n";
    log_file_.flush();
}

}
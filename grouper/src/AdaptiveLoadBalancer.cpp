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
            log_file_ << "timestamp_ms,group_id,selected_cu,is_warmup,concurrent_on_cu,pred_latency_ms,actual_latency_ms,pred_ram_bytes,actual_ram_bytes";
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
    if (retrain_thread_.joinable()) {
        retrain_thread_.join();
    }
    if (!config_.latency_model_path.empty() || !config_.ram_model_path.empty()) {
        xgb_->saveModels(config_.latency_model_path, config_.ram_model_path);
    }
}

uint64_t AdaptiveLoadBalancer::assignGroup(uint64_t group_id,
                                           const std::vector<const plan::PlanDAG*>& dags,
                                           const std::vector<uint64_t>& available_cus,
                                           const std::unordered_map<std::string, uint64_t>& schema) {
    if (available_cus.empty()) {
        std::cout << "[LOAD-BALANCER ERROR]: available_cus is empty!" << std::endl;
        return 0;
    }

    std::vector<float> query_features = QueryVectorizer::vectorizeGroup(dags, schema);
    bool ready = is_ready_.load(std::memory_order_acquire);
    auto cu_states = metrics_client_->getCurrentStates();

    ExecutionMetrics prediction{0.0, 0.0};
    uint64_t selected_cu = 0;
    std::vector<float> selected_context(BANDIT_FEATURES, 0.0f);

    std::unordered_map<uint64_t, size_t> current_in_flight;
    {
        std::shared_lock lock(tracking_mutex_);
        current_in_flight = cu_in_flight_;
    }

    if (!ready) {
        size_t idx = rr_counter_.fetch_add(1, std::memory_order_relaxed) % available_cus.size();
        selected_cu = available_cus[idx];
    } else {
        prediction = xgb_->predict(query_features);

        std::vector<uint64_t> valid_cus;
        for (uint64_t cu : available_cus) {
            if (!cu_states.contains(cu) || cu_states[cu].ram_available_bytes >= prediction.ram_peak_bytes * 1.1) {
                valid_cus.push_back(cu);
            }
        }
        const auto& candidate_cus = valid_cus.empty() ? available_cus : valid_cus;

        std::unordered_map<uint64_t, std::vector<float>> cu_bandit_contexts;
        for (uint64_t cu : candidate_cus) {
            float cpu_norm = 0.0f;
            float ram_avail_gb = 0.0f;
            if (cu_states.contains(cu)) {
                cpu_norm = static_cast<float>(cu_states[cu].cpu_usage_percent / 100.0);
                ram_avail_gb = static_cast<float>(cu_states[cu].ram_available_bytes / (1024.0 * 1024.0 * 1024.0));
            }
            float in_flight = static_cast<float>(current_in_flight[cu]);
            float pred_lat_sec = static_cast<float>(prediction.latency_ms / 1000.0);
            float pred_ram_gb = static_cast<float>(prediction.ram_peak_bytes / (1024.0 * 1024.0 * 1024.0));

            cu_bandit_contexts[cu] = {
                pred_lat_sec,
                pred_ram_gb,
                cpu_norm,
                ram_avail_gb,
                in_flight
            };
        }

        selected_cu = sampler_->selectTarget(candidate_cus, cu_bandit_contexts);
        selected_context = std::move(cu_bandit_contexts[selected_cu]);
    }

    double baseline_ram = cu_states.contains(selected_cu) ? cu_states[selected_cu].ram_used_bytes : 0.0;

    {
        std::unique_lock lock(tracking_mutex_);
        size_t active_on_cu = ++cu_in_flight_[selected_cu];

        for (auto& [gid, pg] : pending_groups_) {
            if (pg.selected_cu == selected_cu) {
                pg.max_concurrent_on_cu = std::max(pg.max_concurrent_on_cu, active_on_cu);
            }
        }

        pending_groups_[group_id] = PendingGroup{
            group_id,
            selected_cu,
            std::move(query_features),
            std::move(selected_context),
            prediction,
            baseline_ram,
            std::chrono::steady_clock::now(),
            currentEpochMs(),
            dags.size(),
            active_on_cu,
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

            uint64_t cu = grp_it->second.selected_cu;
            if (cu_in_flight_[cu] > 0) {
                cu_in_flight_[cu]--;
            }

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

    double raw_peak_ram = metrics_client_->getPeakRamInWindow(
        group_info.selected_cu,
        group_info.start_sys_ms,
        end_sys_ms
    );

    double concurrency = static_cast<double>(std::max<size_t>(group_info.max_concurrent_on_cu, 1));
    double ram_delta = (group_info.baseline_ram_bytes > 0.0)
                       ? std::max(0.0, raw_peak_ram - group_info.baseline_ram_bytes)
                       : raw_peak_ram;

    double attributed_latency_ms = actual_latency_ms / concurrency;
    double attributed_ram_bytes = ram_delta / concurrency;

    ExecutionMetrics actuals{attributed_latency_ms, attributed_ram_bytes};
    logGroupExecution(group_info, actuals);

    if (!group_info.was_warmup) {
        double predicted_lat = std::max(group_info.prediction.latency_ms, 1.0);
        double actual_lat = std::max(actual_latency_ms, 1.0);
        double reward = std::clamp(predicted_lat / actual_lat, 0.0, 1.0);
        sampler_->updateDistribution(group_info.bandit_context, reward);
    }

    if (config_.filter_overlapped_for_xgb && group_info.max_concurrent_on_cu > 1) {
        return;
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
        history_latencies_.push_back(static_cast<float>(attributed_latency_ms));
        history_rams_.push_back(static_cast<float>(attributed_ram_bytes));
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

        if (trigger_retrain && !is_retraining_.load(std::memory_order_relaxed)) {
            snapshot_features = history_features_;
            snapshot_latencies = history_latencies_;
            snapshot_rams = history_rams_;
            sample_count = completed_groups_;
        } else {
            trigger_retrain = false;
        }
    }

    if (trigger_retrain) {
        launchAsyncRetrain(
            std::move(snapshot_features),
            std::move(snapshot_latencies),
            std::move(snapshot_rams),
            sample_count,
            finished_warmup
        );
    }
}

void AdaptiveLoadBalancer::launchAsyncRetrain(std::vector<float> features,
                                              std::vector<float> latencies,
                                              std::vector<float> rams,
                                              size_t count,
                                              bool is_warmup_finish) {
    bool expected = false;
    if (!is_retraining_.compare_exchange_strong(expected, true)) {
        return;
    }

    if (retrain_thread_.joinable()) {
        retrain_thread_.join();
    }

    retrain_thread_ = std::thread([this,
                                   feat = std::move(features),
                                   lat = std::move(latencies),
                                   ram = std::move(rams),
                                   count,
                                   is_warmup_finish]() {
        xgb_->retrainModels(feat, lat, ram, count);
        if (is_warmup_finish) {
            is_ready_.store(true, std::memory_order_release);
            std::cout << "[AdaptiveLoadBalancer] Warm-up completed (" << count << " groups). Estimators active.\n";
        }
        is_retraining_.store(false, std::memory_order_release);
    });
}

void AdaptiveLoadBalancer::logGroupExecution(const PendingGroup& group_info, const ExecutionMetrics& actuals) {
    if (!log_file_.is_open()) return;

    std::lock_guard lock(log_mutex_);
    log_file_ << group_info.start_sys_ms << ","
              << group_info.group_id << ","
              << group_info.selected_cu << ","
              << (group_info.was_warmup ? 1 : 0) << ","
              << group_info.max_concurrent_on_cu << ","
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
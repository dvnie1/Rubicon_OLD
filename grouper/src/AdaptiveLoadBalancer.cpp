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
        XGB_FEATURES,
        config_.num_trees
    );
    sampler_ = std::make_unique<ThompsonSampler>(BANDIT_FEATURES, 1.0);

    is_ready_ = !config_.enable_warmup || xgb_->hasLoadedModels();

    if (!config_.log_csv_path.empty()) {
        bool exists = std::filesystem::exists(config_.log_csv_path);
        log_file_.open(config_.log_csv_path, std::ios::app);
        if (log_file_.is_open() && !exists) {
            log_file_ << "timestamp_ms,group_id,selected_cu,is_warmup,concurrent_on_cu,"
                      << "pred_latency_ms,actual_latency_ms,pred_ram_bytes,actual_ram_bytes,"
                      << "pred_iso_latency_ms,pred_iso_ram_bytes";
            for (size_t i = 0; i < XGB_FEATURES; ++i) {
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

    std::vector<float> base_query_features = QueryVectorizer::vectorizeGroup(dags, schema);
    bool ready = is_ready_.load(std::memory_order_acquire);
    auto cu_states = metrics_client_->getCurrentStates();

    uint64_t selected_cu = 0;
    ExecutionMetrics selected_prediction{0.0, 0.0};
    ExecutionMetrics isolated_prediction{0.0, 0.0};
    std::vector<float> selected_context(BANDIT_FEATURES, 0.0f);
    std::vector<float> selected_query_features;

    std::unordered_map<uint64_t, size_t> current_in_flight;
    std::unordered_map<uint64_t, size_t> current_workers;
    std::unordered_map<uint64_t, double> current_reserved_ram;
    std::unordered_map<uint64_t, double> current_reserved_lat;
    {
        std::shared_lock lock(tracking_mutex_);
        current_in_flight = cu_in_flight_;
        current_workers = cu_workers_;
        current_reserved_ram = cu_reserved_ram_;
        current_reserved_lat = cu_reserved_lat_;
    }

    if (!ready) {
        size_t idx = rr_counter_.fetch_add(1, std::memory_order_relaxed) % available_cus.size();
        selected_cu = available_cus[idx];
        selected_query_features = std::move(base_query_features);
        selected_query_features.push_back(static_cast<float>(current_workers[selected_cu]));
    } else {
        std::unordered_map<uint64_t, ExecutionMetrics> cu_preds;
        std::unordered_map<uint64_t, std::vector<float>> cu_q_feats;
        std::vector<uint64_t> valid_cus;

        for (uint64_t cu : available_cus) {
            std::vector<float> q_feat = base_query_features;
            q_feat.push_back(static_cast<float>(current_workers[cu]));

            ExecutionMetrics pred = xgb_->predict(q_feat);
            cu_preds[cu] = pred;
            cu_q_feats[cu] = std::move(q_feat);

            double expected_total_ram = current_reserved_ram[cu] + pred.ram_peak_bytes;
            if (!cu_states.contains(cu) || cu_states[cu].ram_available_bytes >= expected_total_ram * 1.1) {
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

            double effective_free_ram_gb = std::max(0.0, ram_avail_gb - (current_reserved_ram[cu] / (1024.0 * 1024.0 * 1024.0)));
            double expected_queue_lat_sec = (current_reserved_lat[cu] + cu_preds[cu].latency_ms) / 1000.0;
            float pred_ram_gb = static_cast<float>(cu_preds[cu].ram_peak_bytes / (1024.0 * 1024.0 * 1024.0));
            float in_flight = static_cast<float>(current_in_flight[cu]);
            float workers = static_cast<float>(current_workers[cu]);

            cu_bandit_contexts[cu] = {
                static_cast<float>(expected_queue_lat_sec),
                pred_ram_gb,
                cpu_norm,
                static_cast<float>(effective_free_ram_gb),
                in_flight,
                workers
            };
        }

        selected_cu = sampler_->selectTarget(candidate_cus, cu_bandit_contexts);
        isolated_prediction = cu_preds[selected_cu];

        selected_prediction.latency_ms = current_reserved_lat[selected_cu] + isolated_prediction.latency_ms;
        selected_prediction.ram_peak_bytes = current_reserved_ram[selected_cu] + isolated_prediction.ram_peak_bytes;

        selected_query_features = std::move(cu_q_feats[selected_cu]);
        selected_context = std::move(cu_bandit_contexts[selected_cu]);
    }

    double baseline_ram = cu_states.contains(selected_cu) ? cu_states[selected_cu].ram_used_bytes : 0.0;

    {
        std::unique_lock lock(tracking_mutex_);
        size_t active_on_cu = ++cu_in_flight_[selected_cu];
        cu_reserved_ram_[selected_cu] += isolated_prediction.ram_peak_bytes;
        cu_reserved_lat_[selected_cu] += isolated_prediction.latency_ms;

        for (auto& [gid, pg] : pending_groups_) {
            if (pg.selected_cu == selected_cu) {
                pg.max_concurrent_on_cu = std::max(pg.max_concurrent_on_cu, active_on_cu);
            }
        }

        pending_groups_[group_id] = PendingGroup{
            group_id,
            selected_cu,
            std::move(selected_query_features),
            std::move(selected_context),
            selected_prediction,
            isolated_prediction,
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
            cu_reserved_ram_[cu] = std::max(0.0, cu_reserved_ram_[cu] - grp_it->second.isolated_prediction.ram_peak_bytes);
            cu_reserved_lat_[cu] = std::max(0.0, cu_reserved_lat_[cu] - grp_it->second.isolated_prediction.latency_ms);

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

    ExecutionMetrics actuals{actual_latency_ms, raw_peak_ram};
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
        history_latencies_.push_back(static_cast<float>(actual_latency_ms));
        history_rams_.push_back(static_cast<float>(raw_peak_ram));
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

void AdaptiveLoadBalancer::onWorkerCount(uint64_t uuid, size_t workers) {
    std::unique_lock lock(tracking_mutex_);
    cu_workers_[uuid] = workers;
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
              << actuals.ram_peak_bytes << ","
              << group_info.isolated_prediction.latency_ms << ","
              << group_info.isolated_prediction.ram_peak_bytes;

    for (float f : group_info.query_features) {
        log_file_ << "," << f;
    }
    log_file_ << "\n";
    log_file_.flush();
}

} // namespace lb
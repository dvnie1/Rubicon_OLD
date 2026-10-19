#include "AdaptiveLoadBalancer.hpp"
#include "QueryVectorizer.hpp"
#include <algorithm>

namespace lb {
    AdaptiveLoadBalancer::AdaptiveLoadBalancer(const std::string &vm_url, const std::string &latency_model,
                                               const std::string &ram_model) {
        metrics_client_ = std::make_unique<MetricsClient>(vm_url);
        metrics_client_->startPolling(1000);
        xgb_ = std::make_unique<XgbEstimator>(latency_model, ram_model);
        sampler_ = std::make_unique<ThompsonSampler>(BANDIT_FEATURES, 1.0);
    }

    AdaptiveLoadBalancer::~AdaptiveLoadBalancer() {
        metrics_client_->stopPolling();
    }

    uint64_t AdaptiveLoadBalancer::assignGroup(uint64_t group_id,
                                               const std::vector<const plan::PlanDAG *> &dags,
                                               const std::vector<uint64_t> &available_cus,
                                               const std::unordered_map<std::string, uint64_t> &schema) {

        if (available_cus.empty()) return 0;

        std::vector<float> query_features = QueryVectorizer::vectorizeGroup(dags, schema);

        ExecutionMetrics prediction = xgb_->predict(query_features);
        std::cout << "Prediction: " << prediction.latency_ms << "ms " << prediction.ram_peak_bytes << " bytes" <<
                std::endl;

        auto cu_states = metrics_client_->getCurrentStates();

        std::unordered_map<uint64_t, std::vector<float> > cu_bandit_contexts;
        for (uint64_t cu: available_cus) {
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

        uint64_t selected_cu = sampler_->selectTarget(available_cus, cu_bandit_contexts);
        std::cout << "Selected Cu: " << selected_cu << std::endl;

        {
            std::unique_lock lock(tracking_mutex_);
            pending_groups_[group_id] = PendingGroup{
                selected_cu,
                std::move(query_features),
                std::move(cu_bandit_contexts[selected_cu]),
                prediction,
                std::chrono::steady_clock::now(),
                dags.size(),
                true
            };
            for (const auto *dag: dags) {
                plan_to_group_[dag->getPlanId()] = group_id;
            }
        }

        return selected_cu;
    }

    void AdaptiveLoadBalancer::onPlanCompleted(uint32_t plan_id, bool success) {
        std::cout << "AdaptiveLoadBalancer::onPlanCompleted" << std::endl;
        PendingGroup finished_group;
        bool group_done = false;
        double actual_latency_ms = 0.0;

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
                auto now = std::chrono::steady_clock::now();
                actual_latency_ms = std::chrono::duration<double, std::milli>(now - grp_it->second.start_time).count();
                finished_group = std::move(grp_it->second);
                pending_groups_.erase(grp_it);
                group_done = true;
            }
        }

        if (group_done) {
            finalizeGroup(finished_group, actual_latency_ms);
        }
    }

    void AdaptiveLoadBalancer::finalizeGroup(const PendingGroup &group_info, double actual_latency_ms) {
        std::cout << "AdaptiveLoadBalancer::finalizeGroup" << std::endl;
        if (!group_info.all_succeeded) {
            sampler_->updateDistribution(group_info.bandit_context, 0.0);
            return;
        }

        double actual_ram_bytes = 0.0;
        auto cu_states = metrics_client_->getCurrentStates();
        if (cu_states.contains(group_info.selected_cu)) {
            actual_ram_bytes = cu_states[group_info.selected_cu].ram_available_bytes;
        }

        ExecutionMetrics actuals{actual_latency_ms, actual_ram_bytes};
        xgb_->updateModels(group_info.query_features, actuals);

        double predicted_lat = std::max(group_info.prediction.latency_ms, 1.0);
        double actual_lat = std::max(actual_latency_ms, 1.0);
        double reward = std::clamp(predicted_lat / actual_lat, 0.0, 1.0);

        sampler_->updateDistribution(group_info.bandit_context, reward);
    }
}

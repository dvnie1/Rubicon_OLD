#include "ThompsonSampler.hpp"
#include <algorithm>

namespace lb {

ThompsonSampler::ThompsonSampler() {
    std::random_device rd;
    rng_.seed(rd());
}

double ThompsonSampler::sampleBeta(double alpha, double beta_param) {
    std::gamma_distribution<double> x_dist(alpha, 1.0);
    std::gamma_distribution<double> y_dist(beta_param, 1.0);
    
    double x = x_dist(rng_);
    double y = y_dist(rng_);
    
    if (x + y == 0.0) return 0.5;
    return x / (x + y);
}

uint64_t ThompsonSampler::selectTarget(const std::vector<uint64_t>& available_cus,
                                       const ExecutionMetrics& prediction,
                                       const std::unordered_map<uint64_t, CUState>& cu_states) {
    if (available_cus.empty()) return 0;

    std::shared_lock lock(mutex_);

    uint64_t best_cu = available_cus[0];
    double best_score = -1.0;

    for (uint64_t cu_uuid : available_cus) {
        auto state_it = cu_states.find(cu_uuid);
        if (state_it != cu_states.end()) {
            if (state_it->second.ram_available_bytes < prediction.ram_peak_bytes * 1.1) {
                continue;
            }
        }

        double alpha = 1.0;
        double beta_param = 1.0;
        auto bandit_it = bandits_.find(cu_uuid);
        
        if (bandit_it != bandits_.end()) {
            alpha = bandit_it->second.alpha;
            beta_param = bandit_it->second.beta_param;
        }

        double theta = sampleBeta(alpha, beta_param);
        double predicted_lat = std::max(prediction.latency_ms, 1.0);
        double score = theta / predicted_lat;

        if (score > best_score) {
            best_score = score;
            best_cu = cu_uuid;
        }
    }

    if (best_score == -1.0) {
        return available_cus[rng_() % available_cus.size()];
    }

    return best_cu;
}

void ThompsonSampler::updateDistribution(uint64_t cu_uuid, bool success, double actual_latency_ms, double predicted_latency_ms) {
    std::unique_lock lock(mutex_);

    BanditState& bandit = bandits_[cu_uuid];

    if (!success) {
        bandit.beta_param += 1.0;
    } else {
        double ratio = predicted_latency_ms / std::max(actual_latency_ms, 1.0);
        double reward = std::clamp(ratio, 0.0, 1.0);

        bandit.alpha += reward;
        bandit.beta_param += (1.0 - reward);
    }
}

}
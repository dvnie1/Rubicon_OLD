#pragma once

#include <vector>
#include <cstdint>
#include <unordered_map>
#include <random>
#include <shared_mutex>

#include "MetricsClient.hpp"
#include "XgbEstimator.hpp"

namespace lb {

    struct BanditState {
        double alpha = 1.0;
        double beta_param = 1.0;
    };

    class ThompsonSampler {
    public:
        ThompsonSampler();

        uint64_t selectTarget(const std::vector<uint64_t>& available_cus,
                              const ExecutionMetrics& prediction,
                              const std::unordered_map<uint64_t, CUState>& cu_states);

        void updateDistribution(uint64_t cu_uuid, bool success, double actual_latency_ms, double predicted_latency_ms);

    private:
        double sampleBeta(double alpha, double beta_param);

        std::unordered_map<uint64_t, BanditState> bandits_;
        mutable std::shared_mutex mutex_;
        std::mt19937 rng_;
    };

}
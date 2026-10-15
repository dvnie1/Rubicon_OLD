#pragma once

#include <vector>
#include <cstdint>
#include <unordered_map>
#include <random>
#include <shared_mutex>
#include <Eigen/Dense>

namespace lb {

    class ThompsonSampler {
    public:
        ThompsonSampler(size_t n_features, double v = 1.0);

        uint64_t selectTarget(const std::vector<uint64_t>& available_cus,
                              const std::unordered_map<uint64_t, std::vector<float>>& cu_features);

        void updateDistribution(const std::vector<float>& context, double reward);

    private:
        size_t n_features_;
        double v_;

        Eigen::MatrixXd B_;
        Eigen::VectorXd f_;
        Eigen::VectorXd mu_;
        Eigen::MatrixXd B_inv_;

        mutable std::shared_mutex mutex_;
        std::mt19937 rng_;
        std::normal_distribution<double> norm_dist_;
    };

}
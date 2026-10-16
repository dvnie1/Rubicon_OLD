#include "ThompsonSampler.hpp"
#include <limits>
#include <mutex>

namespace lb {

    ThompsonSampler::ThompsonSampler(size_t n_features, double v)
        : n_features_(n_features), v_(v), norm_dist_(0.0, 1.0) {
        std::random_device rd;
        rng_.seed(rd());

        B_ = Eigen::MatrixXd::Identity(n_features_, n_features_);
        f_ = Eigen::VectorXd::Zero(n_features_);
        mu_ = Eigen::VectorXd::Zero(n_features_);
        B_inv_ = Eigen::MatrixXd::Identity(n_features_, n_features_);
    }

    uint64_t ThompsonSampler::selectTarget(const std::vector<uint64_t>& available_cus,
                                           const std::unordered_map<uint64_t, std::vector<float>>& cu_features) {
        if (available_cus.empty()) return 0;

        std::shared_lock lock(mutex_);

        Eigen::MatrixXd cov = (v_ * v_) * B_inv_;
        Eigen::LLT<Eigen::MatrixXd> llt(cov);
        Eigen::MatrixXd L = llt.matrixL();

        Eigen::VectorXd z(n_features_);
        for (size_t i = 0; i < n_features_; ++i) {
            z(i) = norm_dist_(rng_);
        }

        Eigen::VectorXd theta_sample = mu_ + L * z;

        double best_score = -std::numeric_limits<double>::infinity();
        uint64_t best_cu = available_cus[0];

        for (uint64_t cu_uuid : available_cus) {
            auto it = cu_features.find(cu_uuid);
            if (it == cu_features.end()) continue;

            Eigen::VectorXd feat = Eigen::Map<const Eigen::VectorXf>(it->second.data(), it->second.size()).cast<double>();
            double score = feat.dot(theta_sample);

            if (score > best_score) {
                best_score = score;
                best_cu = cu_uuid;
            }
        }

        return best_cu;
    }

    void ThompsonSampler::updateDistribution(const std::vector<float>& context, double reward) {
        std::unique_lock lock(mutex_);

        Eigen::VectorXd x = Eigen::Map<const Eigen::VectorXf>(context.data(), context.size()).cast<double>();

        B_ += x * x.transpose();
        f_ += x * reward;
        B_inv_ = B_.inverse();
        mu_ = B_inv_ * f_;
    }

}
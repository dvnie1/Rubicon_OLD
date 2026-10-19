#pragma once

#include <vector>
#include <string>
#include <shared_mutex>
#include <xgboost/c_api.h>

namespace lb {
    struct ExecutionMetrics {
        double latency_ms = 0.0;
        double ram_peak_bytes = 0.0;
    };

    class XgbEstimator {
    public:
        XgbEstimator(const std::string &latency_model_path, const std::string &ram_model_path, size_t num_features,
                     size_t num_trees = 100);

        ~XgbEstimator();

        XgbEstimator(const XgbEstimator &) = delete;
        XgbEstimator &operator=(const XgbEstimator &) = delete;

        ExecutionMetrics predict(const std::vector<float> &features) const;

        void retrainModels(const std::vector<float> &flat_features,
                           const std::vector<float> &latencies,
                           const std::vector<float> &rams,
                           size_t num_samples);
        void saveModels(const std::string &latency_path, const std::string &ram_path) const;
        bool hasLoadedModels() const { return models_loaded_; }

    private:
        BoosterHandle createConfiguredBooster() const;

        BoosterHandle trainNewBooster(DMatrixHandle dmat, const std::vector<float> &labels) const;

        float predictSingle(BoosterHandle handle, DMatrixHandle dmat) const;

        size_t num_features_;
        size_t num_trees_;
        bool models_loaded_{false};

        BoosterHandle latency_booster_{nullptr};
        BoosterHandle ram_booster_{nullptr};

        mutable std::shared_mutex mutex_;
    };
}

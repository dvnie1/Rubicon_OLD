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
        XgbEstimator(const std::string& latency_model_path = "", const std::string& ram_model_path = "");
        ~XgbEstimator();

        XgbEstimator(const XgbEstimator&) = delete;
        XgbEstimator& operator=(const XgbEstimator&) = delete;

        ExecutionMetrics predict(const std::vector<float>& features) const;
        void updateModels(const std::vector<float>& features, const ExecutionMetrics& actuals);
        void saveModels(const std::string& latency_path, const std::string& ram_path) const;

    private:
        void initBooster(BoosterHandle& handle, const std::string& path);
        float predictSingle(BoosterHandle handle, DMatrixHandle dmat) const;
        void updateSingle(BoosterHandle handle, DMatrixHandle dmat, float label, int& iter);

        BoosterHandle latency_booster_{nullptr};
        BoosterHandle ram_booster_{nullptr};

        int latency_iter_{0};
        int ram_iter_{0};

        mutable std::shared_mutex mutex_;
    };

}
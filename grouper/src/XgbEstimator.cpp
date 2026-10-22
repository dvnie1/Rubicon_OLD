#include "XgbEstimator.hpp"
#include <filesystem>
#include <iostream>
#include <cmath>
#include <mutex>
#include <utility>

namespace lb {

XgbEstimator::XgbEstimator(const std::string& latency_model_path, const std::string& ram_model_path, size_t num_features, size_t num_trees)
    : num_features_(num_features), num_trees_(num_trees) {

    bool lat_exists = !latency_model_path.empty() && std::filesystem::exists(latency_model_path);
    bool ram_exists = !ram_model_path.empty() && std::filesystem::exists(ram_model_path);

    if (lat_exists && ram_exists) {
        XGBoosterCreate(nullptr, 0, &latency_booster_);
        XGBoosterCreate(nullptr, 0, &ram_booster_);
        if (XGBoosterLoadModel(latency_booster_, latency_model_path.c_str()) == 0 &&
            XGBoosterLoadModel(ram_booster_, ram_model_path.c_str()) == 0) {
            models_loaded_ = true;
            return;
        }
    }

    latency_booster_ = createConfiguredBooster();
    ram_booster_ = createConfiguredBooster();
}

XgbEstimator::~XgbEstimator() {
    std::unique_lock lock(mutex_);
    if (latency_booster_) XGBoosterFree(latency_booster_);
    if (ram_booster_) XGBoosterFree(ram_booster_);
}

BoosterHandle XgbEstimator::createConfiguredBooster() const {
    BoosterHandle handle = nullptr;
    XGBoosterCreate(nullptr, 0, &handle);
    std::string num_feat_str = std::to_string(num_features_);
    XGBoosterSetParam(handle, "num_feature", num_feat_str.c_str());
    XGBoosterSetParam(handle, "objective", "reg:squarederror");
    XGBoosterSetParam(handle, "eta", "0.05");
    XGBoosterSetParam(handle, "max_depth", "6");
    return handle;
}

BoosterHandle XgbEstimator::trainNewBooster(DMatrixHandle dmat, const std::vector<float>& labels) const {
    XGDMatrixSetFloatInfo(dmat, "label", labels.data(), labels.size());

    DMatrixHandle eval_dmats[1] = { dmat };
    BoosterHandle handle = nullptr;
    XGBoosterCreate(eval_dmats, 1, &handle);

    std::string num_feat_str = std::to_string(num_features_);
    XGBoosterSetParam(handle, "num_feature", num_feat_str.c_str());
    XGBoosterSetParam(handle, "objective", "reg:squarederror");
    XGBoosterSetParam(handle, "eta", "0.05");
    XGBoosterSetParam(handle, "max_depth", "6");

    for (size_t i = 0; i < num_trees_; ++i) {
        if (XGBoosterUpdateOneIter(handle, static_cast<int>(i), dmat) != 0) {
            std::cerr << "[XgbEstimator] Retrain error at iter " << i << ": " << XGBGetLastError() << "\n";
            break;
        }
    }
    return handle;
}

void XgbEstimator::retrainModels(const std::vector<float>& flat_features,
                                 const std::vector<float>& latencies,
                                 const std::vector<float>& rams,
                                 size_t num_samples) {
    if (num_samples == 0 || flat_features.size() != num_samples * num_features_) return;

    DMatrixHandle dmat = nullptr;
    XGDMatrixCreateFromMat(flat_features.data(), num_samples, num_features_, NAN, &dmat);

    BoosterHandle new_lat_booster = trainNewBooster(dmat, latencies);
    BoosterHandle new_ram_booster = trainNewBooster(dmat, rams);

    XGDMatrixFree(dmat);

    {
        std::unique_lock lock(mutex_);
        std::swap(latency_booster_, new_lat_booster);
        std::swap(ram_booster_, new_ram_booster);
    }

    if (new_lat_booster) XGBoosterFree(new_lat_booster);
    if (new_ram_booster) XGBoosterFree(new_ram_booster);
}

ExecutionMetrics XgbEstimator::predict(const std::vector<float>& features) const {
    if (features.empty()) {
        std::cout << "[XGB ERROR]: received empty features. Returning {0, 0}." << std::endl;
        return {0.0, 0.0};
    }

    std::shared_lock lock(mutex_);

    DMatrixHandle dmat = nullptr;
    XGDMatrixCreateFromMat(features.data(), 1, features.size(), NAN, &dmat);

    ExecutionMetrics result;
    result.latency_ms = static_cast<double>(predictSingle(latency_booster_, dmat));
    result.ram_peak_bytes = static_cast<double>(predictSingle(ram_booster_, dmat));

    XGDMatrixFree(dmat);
    return result;
}

float XgbEstimator::predictSingle(BoosterHandle handle, DMatrixHandle dmat) const {
    bst_ulong out_len = 0;
    const float* out_result = nullptr;
    if (XGBoosterPredict(handle, dmat, 0, 0, 0, &out_len, &out_result) == 0 && out_len > 0) {
        return out_result[0];
    }
    return 0.0f;
}

void XgbEstimator::saveModels(const std::string& latency_path, const std::string& ram_path) const {
    std::shared_lock lock(mutex_);
    if (!latency_path.empty()) XGBoosterSaveModel(latency_booster_, latency_path.c_str());
    if (!ram_path.empty()) XGBoosterSaveModel(ram_booster_, ram_path.c_str());
}

}
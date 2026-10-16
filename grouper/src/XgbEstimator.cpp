#include "XgbEstimator.hpp"
#include <filesystem>
#include <iostream>
#include <cmath>
#include <mutex>

namespace lb {

XgbEstimator::XgbEstimator(const std::string& latency_model_path, const std::string& ram_model_path) {
    initBooster(latency_booster_, latency_model_path);
    initBooster(ram_booster_, ram_model_path);
}

XgbEstimator::~XgbEstimator() {
    std::unique_lock lock(mutex_);
    if (latency_booster_) XGBoosterFree(latency_booster_);
    if (ram_booster_) XGBoosterFree(ram_booster_);
}

void XgbEstimator::initBooster(BoosterHandle& handle, const std::string& path) {
    XGBoosterCreate(nullptr, 0, &handle);
    
    if (!path.empty() && std::filesystem::exists(path)) {
        if (XGBoosterLoadModel(handle, path.c_str()) != 0) {
            std::cerr << "[XgbEstimator] Failed to load model: " << XGBGetLastError() << "\n";
        }
    } else {
        XGBoosterSetParam(handle, "objective", "reg:squarederror");
        XGBoosterSetParam(handle, "eta", "0.05");
        XGBoosterSetParam(handle, "max_depth", "4");
    }
}

ExecutionMetrics XgbEstimator::predict(const std::vector<float>& features) const {
    if (features.empty()) return {0.0, 0.0};

    std::shared_lock lock(mutex_);
    
    DMatrixHandle dmat;
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

void XgbEstimator::updateModels(const std::vector<float>& features, const ExecutionMetrics& actuals) {
    if (features.empty()) return;

    std::unique_lock lock(mutex_);

    DMatrixHandle dmat;
    XGDMatrixCreateFromMat(features.data(), 1, features.size(), NAN, &dmat);

    updateSingle(latency_booster_, dmat, static_cast<float>(actuals.latency_ms), latency_iter_);
    updateSingle(ram_booster_, dmat, static_cast<float>(actuals.ram_peak_bytes), ram_iter_);

    XGDMatrixFree(dmat);
}

void XgbEstimator::updateSingle(BoosterHandle handle, DMatrixHandle dmat, float label, int& iter) {
    XGDMatrixSetFloatInfo(dmat, "label", &label, 1);
    if (XGBoosterUpdateOneIter(handle, iter, dmat) == 0) {
        iter++;
    } else {
        std::cerr << "[XgbEstimator] Update error: " << XGBGetLastError() << "\n";
    }
}

void XgbEstimator::saveModels(const std::string& latency_path, const std::string& ram_path) const {
    std::shared_lock lock(mutex_);
    if (!latency_path.empty()) XGBoosterSaveModel(latency_booster_, latency_path.c_str());
    if (!ram_path.empty()) XGBoosterSaveModel(ram_booster_, ram_path.c_str());
}

}
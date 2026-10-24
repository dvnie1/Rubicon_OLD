#include "MetricsClient.hpp"
#include <chrono>
#include <iostream>
#include <algorithm>
#include "httplib.h"
#include "nlohmann/json.hpp"

using json = nlohmann::json;

namespace lb {

MetricsClient::MetricsClient(std::string vm_url)
    : vm_url_(std::move(vm_url)), interval_ms_(1000) {
    std::cout << "[MetricsClient] Initialized!" << std::endl;
}

MetricsClient::~MetricsClient() {
    stopPolling();
}

void MetricsClient::startPolling(uint32_t interval_ms) {
    if (active_.exchange(true)) return;
    interval_ms_ = interval_ms;
    poll_thread_ = std::thread(&MetricsClient::pollLoop, this);
}

void MetricsClient::stopPolling() {
    if (active_.exchange(false)) {
        if (poll_thread_.joinable()) {
            poll_thread_.join();
        }
    }
}

std::unordered_map<uint64_t, CUState> MetricsClient::getCurrentStates() const {
    std::shared_lock lock(state_mutex_);
    return current_states_;
}

void MetricsClient::pollLoop() {
    std::cout << "[MetricsClient] Starting polling thread..." << std::endl;
    while (active_) {
        fetchAndParseMetrics();
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
    }
}

void MetricsClient::fetchAndParseMetrics() {
    httplib::Client cli(vm_url_);
    std::unordered_map<uint64_t, CUState> new_states;

    auto res_cpu = cli.Get("/api/v1/query?query=avg_over_time(cpu_usage_percent{scope=\"process\"}[5s])");
    if (res_cpu && res_cpu->status == 200) {
        try {
            json j = json::parse(res_cpu->body);
            for (const auto &item : j["data"]["result"]) {
                if (item["metric"].contains("host_unit_uuid")) {
                    uint64_t uuid = std::stoull(item["metric"]["host_unit_uuid"].get<std::string>());
                    new_states[uuid].cpu_usage_percent = std::stod(item["value"][1].get<std::string>());
                }
            }
        } catch (const std::exception &e) {
            std::cerr << "[MetricsClient] JSON parse error (CPU): " << e.what() << "\n";
        }
    }

    auto res_avail = cli.Get("/api/v1/query?query=min_over_time(memory_available_bytes{scope=\"host\"}[5s])");
    if (res_avail && res_avail->status == 200) {
        try {
            json j = json::parse(res_avail->body);
            for (const auto &item : j["data"]["result"]) {
                if (item["metric"].contains("host_unit_uuid")) {
                    uint64_t uuid = std::stoull(item["metric"]["host_unit_uuid"].get<std::string>());
                    new_states[uuid].ram_available_bytes = std::stod(item["value"][1].get<std::string>());
                }
            }
        } catch (const std::exception &e) {
            std::cerr << "[MetricsClient] JSON parse error (RAM Available): " << e.what() << "\n";
        }
    }

    std::unique_lock lock(state_mutex_);
    for (const auto &[uuid, state] : new_states) {
        current_states_[uuid].cpu_usage_percent = state.cpu_usage_percent;
        if (state.ram_available_bytes > 0.0) {
            current_states_[uuid].ram_available_bytes = state.ram_available_bytes;
        }
        current_states_[uuid].ram_used_bytes = 0.0;
    }
}

double MetricsClient::getPeakRamInWindow(uint64_t cu_uuid, int64_t start_ts_ms, int64_t end_ts_ms) const {
    httplib::Client cli(vm_url_);

    int64_t duration_ms = std::max<int64_t>(end_ts_ms - start_ts_ms, 1000);
    double end_sec = static_cast<double>(end_ts_ms) / 1000.0;
    std::string uuid_str = std::to_string(cu_uuid);
    std::string window_str = std::to_string(duration_ms) + "ms";
    std::string time_param = "&time=" + std::to_string(end_sec);

    auto querySingleValue = [&](const std::string &promql) -> double {
        std::string path = "/api/v1/query?query=" + promql + time_param;
        auto res = cli.Get(path.c_str());
        if (res && res->status == 200) {
            try {
                json j = json::parse(res->body);
                const auto &results = j["data"]["result"];
                if (!results.empty()) {
                    return std::stod(results[0]["value"][1].get<std::string>());
                }
            } catch (const std::exception &e) {
                std::cerr << "[MetricsClient] Parse error: " << e.what() << "\n";
            }
        }
        return 0.0;
    };

    std::string inter_max_q = "max_over_time(intermediate_table_size_bytes{scope=\"application\",host_unit_uuid=\"" + uuid_str + "\"}[" + window_str + "])";
    std::string cat_max_q   = "max_over_time(catalog_total_size_bytes{scope=\"application\",host_unit_uuid=\"" + uuid_str + "\"}[" + window_str + "])";
    std::string cat_min_q   = "min_over_time(catalog_total_size_bytes{scope=\"application\",host_unit_uuid=\"" + uuid_str + "\"}[" + window_str + "])";

    double intermediate_peak = querySingleValue(inter_max_q);
    double catalog_max = querySingleValue(cat_max_q);
    double catalog_min = querySingleValue(cat_min_q);

    double catalog_delta = (catalog_max > catalog_min && catalog_min > 0.0) ? (catalog_max - catalog_min) : 0.0;

    return std::max(intermediate_peak, catalog_delta);
}

}
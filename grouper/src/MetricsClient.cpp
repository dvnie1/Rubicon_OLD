#include "MetricsClient.hpp"
#include <chrono>
#include <iostream>
#include <algorithm>
#include "httplib.h"
#include "nlohmann/json.hpp"

using json = nlohmann::json;

namespace lb {

MetricsClient::MetricsClient(std::string vm_url)
    : vm_url_(std::move(vm_url)), interval_ms_(1000) {}

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

    auto res_used = cli.Get("/api/v1/query?query=last_over_time(memory_used_bytes{scope=\"process\"}[5s])");
    if (res_used && res_used->status == 200) {
        try {
            json j = json::parse(res_used->body);
            for (const auto &item : j["data"]["result"]) {
                if (item["metric"].contains("host_unit_uuid")) {
                    uint64_t uuid = std::stoull(item["metric"]["host_unit_uuid"].get<std::string>());
                    new_states[uuid].ram_used_bytes = std::stod(item["value"][1].get<std::string>());
                }
            }
        } catch (const std::exception &e) {
            std::cerr << "[MetricsClient] JSON parse error (RAM Used): " << e.what() << "\n";
        }
    }

    std::unique_lock lock(state_mutex_);
    for (const auto &[uuid, state] : new_states) {
        current_states_[uuid].cpu_usage_percent = state.cpu_usage_percent;
        current_states_[uuid].ram_available_bytes = state.ram_available_bytes;
        current_states_[uuid].ram_used_bytes = state.ram_used_bytes;
    }
}

double MetricsClient::getPeakRamInWindow(uint64_t cu_uuid, int64_t start_ts_ms, int64_t end_ts_ms) const {
    httplib::Client cli(vm_url_);

    int64_t duration_ms = std::max<int64_t>(end_ts_ms - start_ts_ms, 1000);
    double end_sec = static_cast<double>(end_ts_ms) / 1000.0;

    std::string query_path =
            "/api/v1/query?query=max_over_time(memory_used_bytes{scope=\"process\",host_unit_uuid=\""
            + std::to_string(cu_uuid) + "\"}[" + std::to_string(duration_ms) + "ms])"
            + "&time=" + std::to_string(end_sec);

    auto res = cli.Get(query_path.c_str());
    if (res && res->status == 200) {
        try {
            json j = json::parse(res->body);
            const auto &results = j["data"]["result"];
            if (!results.empty()) {
                return std::stod(results[0]["value"][1].get<std::string>());
            }
        } catch (const std::exception &e) {
            std::cerr << "[MetricsClient] Historical RAM parse error: " << e.what() << "\n";
        }
    }

    auto states = getCurrentStates();
    auto it = states.find(cu_uuid);
    return (it != states.end()) ? it->second.ram_used_bytes : 0.0;
}

}
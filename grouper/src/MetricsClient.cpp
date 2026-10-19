#include "MetricsClient.hpp"
#include <chrono>
#include <iostream>
#include "httplib.h"
#include "nlohmann/json.hpp"
#include <snappy.h>

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

        auto res_cpu = cli.Get("/api/v1/query?query=max_over_time(cpu_usage_percent{scope=\"global\"}[5s])");
        if (res_cpu && res_cpu->status == 200) {
            try {
                json j = json::parse(res_cpu->body);
                for (const auto& item : j["data"]["result"]) {
                    if (item["metric"].contains("host_unit_uuid")) {
                        uint64_t uuid = std::stoull(item["metric"]["host_unit_uuid"].get<std::string>());
                        new_states[uuid].cpu_usage_percent = std::stod(item["value"][1].get<std::string>());
                        std::cout << "CPU state: " << uuid << " " << new_states[uuid].cpu_usage_percent << std::endl;
                    }
                }
            } catch (const std::exception& e) {
                std::cerr << "[MetricsClient] JSON parse error (CPU): " << e.what() << "\n";
            }
        }

        // TODO: memory_used_bytes is not ram_available_bytes !!!
        auto res_ram = cli.Get("/api/v1/query?query=memory_used_bytes{scope=\"global\"}");
        if (res_ram && res_ram->status == 200) {
            try {
                json j = json::parse(res_ram->body);
                for (const auto& item : j["data"]["result"]) {
                    if (item["metric"].contains("host_unit_uuid")) {
                        uint64_t uuid = std::stoull(item["metric"]["host_unit_uuid"].get<std::string>());
                        new_states[uuid].ram_available_bytes = std::stod(item["value"][1].get<std::string>());
                    }
                }
            } catch (const std::exception& e) {
                std::cerr << "[MetricsClient] JSON parse error (RAM): " << e.what() << "\n";
            }
        }

        std::unique_lock lock(state_mutex_);
        for (const auto& [uuid, state] : new_states) {
            current_states_[uuid].cpu_usage_percent = state.cpu_usage_percent;
            if (state.ram_available_bytes > 0) {
                current_states_[uuid].ram_available_bytes = state.ram_available_bytes;
            }
        }
    }

} // namespace lb
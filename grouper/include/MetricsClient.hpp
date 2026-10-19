#pragma once

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <cstdint>

namespace lb {

    struct CUState {
        double cpu_usage_percent = 0.0;
        double ram_available_bytes = 0.0;
    };

    class MetricsClient {
    public:
        explicit MetricsClient(std::string vm_url);
        ~MetricsClient();

        void startPolling(uint32_t interval_ms);
        void stopPolling();

        std::unordered_map<uint64_t, CUState> getCurrentStates() const;
        double getPeakRamInWindow(uint64_t cu_uuid, int64_t start_ts_ms, int64_t end_ts_ms) const;

    private:
        void pollLoop();
        void fetchAndParseMetrics();

        std::string vm_url_;
        uint32_t interval_ms_;

        mutable std::shared_mutex state_mutex_;
        std::unordered_map<uint64_t, CUState> current_states_;

        std::atomic<bool> active_{false};
        std::thread poll_thread_;
    };

}
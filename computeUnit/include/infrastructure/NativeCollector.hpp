#pragma once

#include <chrono>
#include <map>
#include <string>
#include <vector>
#include <memory>
#include <filesystem>
#include <functional>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <cerrno>

#include <StaticSysMetric.pb.h>

// #include <prometheus/registry.h>

struct AppMetricCallbacks {
    std::function<size_t()> get_intermediate_table_size = [](){ return 0; };
    std::function<size_t()> get_column_count = [](){ return 0; };
    std::function<size_t()> get_total_catalog_size = [](){ return 0; };
};

class NativeCollector {
public:

    NativeCollector(AppMetricCallbacks callbacks);
    ~NativeCollector();

    void collect_static_info();
    metrics::DynamicMetricBatch collect_dynamic_metrics(bool collect_global);
    bool try_acquire_leadership();
    bool is_leader() const { return is_global_leader; }

    const metrics::CpuInfoDetailed& get_cpu_info_detailed() const { return cpu_info_detailed_pb; }
    const metrics::CpuInfo& get_cpu_info() const { return cpu_info_pb; }
    const metrics::MemInfoDetailed& get_mem_info_detailed() const { return mem_info_detailed_pb; }
    const metrics::MemInfo& get_mem_info() const { return mem_info_pb; }
    const metrics::NumaInfo& get_numa_info() const { return numa_info_pb; }
    const metrics::PciInfo& get_pci_info() const { return pci_info_pb; }
    const metrics::KernelInfo& get_kernel_info() const { return kernel_info_pb; }
    const metrics::HostMachineUuid& get_host_machine_uuid() const { return host_machine_uuid_pb; }

private:

    AppMetricCallbacks app_callbacks;

    int lock_fd = -1;
    bool is_global_leader = false;
    const std::string lock_file_path = "/tmp/disagg_monitor_global.lock";

    std::function<size_t()> get_intermediate_table_size;

    metrics::CpuInfoDetailed cpu_info_detailed_pb;
    metrics::CpuInfo cpu_info_pb;
    metrics::MemInfoDetailed mem_info_detailed_pb;
    metrics::MemInfo mem_info_pb;
    metrics::NumaInfo numa_info_pb;
    metrics::PciInfo pci_info_pb;
    metrics::KernelInfo kernel_info_pb;
    metrics::HostMachineUuid host_machine_uuid_pb;
    std::string host_machine_uuid_string;

    struct CpuStats {
        unsigned long long user = 0;
        unsigned long long nice = 0;
        unsigned long long system = 0;
        unsigned long long idle = 0;
        unsigned long long iowait = 0;
        unsigned long long irq = 0;
        unsigned long long softirq = 0;
        unsigned long long steal = 0;

        unsigned long long getTotal() const {
            return user + nice + system + idle + iowait + irq + softirq + steal;
        }

        unsigned long long getIdle() const {
            return idle + iowait;
        }
    };

    struct NetDevStats {
        unsigned long long rx_bytes = 0;
        unsigned long long tx_bytes = 0;
    };

    struct IBPortStats {
        unsigned long long xmit_data = 0;
        unsigned long long rcv_data = 0;
        unsigned long long xmit_pkts = 0;
        unsigned long long rcv_pkts = 0;
    };

    struct IOStats {
        unsigned long long rchar = 0;
        unsigned long long wchar = 0;
    };

    bool first_proc_read = true;
    bool first_net_read = true;
    bool first_ib_read = true;
    bool first_io_read = true;

    std::chrono::time_point<std::chrono::steady_clock> last_proc_read_time;
    std::chrono::time_point<std::chrono::steady_clock> last_net_read_time;
    std::chrono::time_point<std::chrono::steady_clock> last_ib_read_time;
    std::chrono::steady_clock::time_point last_io_read_time;

    IOStats prev_io_stats;
    CpuStats prev_global_cpu;
    unsigned long long prev_proc_utime = 0;
    unsigned long long prev_proc_stime = 0;
    std::map<std::string, NetDevStats> prev_net_stats;
    std::map<std::string, IBPortStats> prev_ib_stats;

    void collect_global_cpu(metrics::DynamicMetricBatch& batch, int64_t now_ms);
    void collect_process_cpu(metrics::DynamicMetricBatch& batch, int64_t now_ms);
    void collect_memory_metrics(metrics::DynamicMetricBatch& batch, int64_t now_ms, bool collect_global);
    void collect_network_metrics(metrics::DynamicMetricBatch& batch, int64_t now_ms);
    void collect_infiniband_metrics(metrics::DynamicMetricBatch& batch, int64_t now_ms);
    void collect_app_metrics(metrics::DynamicMetricBatch& batch, int64_t now_ms);
    void collect_process_io(metrics::DynamicMetricBatch &batch, int64_t now_ms);

    void collect_cpu_detailed();
    void collect_cpu();
    void collect_mem_detailed();
    void collect_mem();
    void collect_numa();
    void collect_pci();
    void collect_kernel();
    void collect_host_machine_uuid();

    std::string exec_cmd(const char* cmd);
};
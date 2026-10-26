#include <infrastructure/NativeCollector.hpp>

#include <fstream>
#include <sstream>
#include <cstdio>
#include <unistd.h>
#include <cmath>
#include <iostream>
#include <algorithm>
#include <thread>
#include <google/protobuf/map.h>
#include <sys/utsname.h>
#include <cstdio>

namespace fs = std::filesystem;

static unsigned long long read_sysfs_ull(const fs::path &path) {
    std::ifstream f(path);
    unsigned long long val = 0;
    if (f >> val) return val;
    return 0;
}

static int64_t get_timestamp_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

static std::string trim(const std::string &str) {
    size_t first = str.find_first_not_of(" \t");
    if (std::string::npos == first) return str;
    size_t last = str.find_last_not_of(" \t");
    return str.substr(first, (last - first + 1));
}

static void parse_cpu_detailed(const std::string &raw, metrics::CpuInfoDetailed &out) {
    std::istringstream iss(raw);
    std::string line;
    metrics::CpuInfoDetailed::Processor *current = nullptr;

    while (std::getline(iss, line)) {
        if (line.empty()) {
            current = nullptr;
            continue;
        }
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;

        std::string key = trim(line.substr(0, colon));
        std::string val = trim(line.substr(colon + 1));

        if (key == "processor") current = out.add_processors();
        if (current) current->mutable_fields()->insert({key, val});
    }
}

static void parse_key_value_lines(const std::string &raw, google::protobuf::Map<std::string, std::string> *map) {
    std::istringstream iss(raw);
    std::string line;
    while (std::getline(iss, line)) {
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string key = trim(line.substr(0, colon));
        std::string val = trim(line.substr(colon + 1));
        if (!key.empty()) map->insert({key, val});
    }
}

static void parse_mem_detailed(const std::string &raw, metrics::MemInfoDetailed &out) {
    std::istringstream iss(raw);
    std::string line;
    while (std::getline(iss, line)) {
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string key = trim(line.substr(0, colon));
        std::string val_str = trim(line.substr(colon + 1));

        std::istringstream val_iss(val_str);
        int64_t value;
        val_iss >> value;
        if (!key.empty()) out.mutable_values()->insert({key, value});
    }
}

template<typename T>
static void parse_lines_to_repeated(const std::string &raw, T &out_msg) {
    std::istringstream iss(raw);
    std::string line;
    while (std::getline(iss, line)) {
        if (!line.empty()) out_msg.add_lines(line);
    }
}

NativeCollector::NativeCollector(AppMetricCallbacks callbacks)
    : app_callbacks(callbacks) {
    collect_static_info();
}

NativeCollector::~NativeCollector() {
    if (lock_fd != -1) {
        close(lock_fd);
    }
}

bool NativeCollector::try_acquire_leadership() {
    if (lock_fd == -1) {
        // O_CREAT: Create file, 0666: Permissions
        lock_fd = open(lock_file_path.c_str(), O_RDWR | O_CREAT, 0666);
        if (lock_fd == -1) {
            std::cerr << "[NativeCollector] Failed to open lock file: " << lock_file_path << std::endl;
            return false;
        }
    }

    // try to acquire exclusive lock without blocking
    if (flock(lock_fd, LOCK_EX | LOCK_NB) == 0) {
        if (!is_global_leader) {
            std::cout << "[NativeCollector] Acquired GLOBAL LEADERSHIP. I will collect system metrics." << std::endl;
        }
        is_global_leader = true;
        return true;
    } else {
        if (is_global_leader) {
            std::cout << "[NativeCollector] Lost leadership." << std::endl;
        }
        is_global_leader = false;
        return false;
    }
}

metrics::DynamicMetricBatch NativeCollector::collect_dynamic_metrics(bool collect_global) {
    metrics::DynamicMetricBatch batch;
    int64_t now = get_timestamp_ms();

    if (collect_global) {
        collect_global_cpu(batch, now);
        collect_network_metrics(batch, now);
        collect_infiniband_metrics(batch, now);
    }
    collect_process_cpu(batch, now);
    collect_app_metrics(batch, now);
    collect_process_io(batch, now);
    collect_memory_metrics(batch, now, collect_global);
    return batch;
}

void NativeCollector::collect_global_cpu(metrics::DynamicMetricBatch &batch, int64_t now_ms) {
    std::ifstream f("/proc/stat");
    if (!f.is_open()) return;

    std::string line;
    std::getline(f, line);

    if (line.substr(0, 3) != "cpu") return;

    CpuStats curr;
    std::string label;
    std::stringstream ss(line);

    ss >> label >> curr.user >> curr.nice >> curr.system >> curr.idle >> curr.iowait >> curr.irq >> curr.softirq >> curr
            .steal;

    unsigned long long prev_total = prev_global_cpu.getTotal();
    unsigned long long curr_total = curr.getTotal();
    unsigned long long total_diff = curr_total - prev_total;

    unsigned long long prev_idle = prev_global_cpu.getIdle();
    unsigned long long curr_idle = curr.getIdle();
    unsigned long long idle_diff = curr_idle - prev_idle;

    double usage_pct = 0.0;
    if (total_diff > 0) {
        usage_pct = (double) (total_diff - idle_diff) / total_diff * 100.0;
    }

    prev_global_cpu = curr;

    auto *m = batch.add_metrics();
    m->set_name("cpu_usage_percent");
    (*m->mutable_labels())["scope"] = "global";
    (*m->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
    m->set_value(usage_pct);
    m->set_timestamp_ms(now_ms);
}

void NativeCollector::collect_process_cpu(metrics::DynamicMetricBatch &batch, int64_t now_ms) {
    std::ifstream f("/proc/self/stat");
    if (!f.is_open()) {
        std::cerr << "[NativeCollector] Error: Cannot open /proc/self/stat" << std::endl;
        return;
    }

    std::string content;
    std::getline(f, content);

    size_t last_paren = content.rfind(')');
    if (last_paren == std::string::npos) {
        std::cerr << "[NativeCollector] Error: parsing /proc/self/stat failed (no parenthesis)" << std::endl;
        return;
    }

    std::stringstream ss(content.substr(last_paren + 1));

    std::string temp;
    unsigned long long utime = 0, stime = 0;

    // Fields after ')' are: state, ppid, pgrp, session, tty_nr, tpgid, flags, minflt, cminflt, majflt, cmajflt, utime, stime
    // We have to skip 11
    for (int i = 0; i < 11; i++) ss >> temp;

    ss >> utime >> stime;

    auto now_steady = std::chrono::steady_clock::now();

    if (!first_proc_read) {
        long hertz = sysconf(_SC_CLK_TCK);
        double duration_sec = std::chrono::duration<double>(now_steady - last_proc_read_time).count();

        if (duration_sec > 0) {
            unsigned long long ticks_diff = (utime + stime) - (prev_proc_utime + prev_proc_stime);
            double usage_pct = (double) ticks_diff / hertz / duration_sec * 100.0;

            int n = std::thread::hardware_concurrency();

            // normalize for multicore cpu
            usage_pct = usage_pct / n;

            auto *m = batch.add_metrics();
            m->set_name("cpu_usage_percent");
            (*m->mutable_labels())["scope"] = "process";
            (*m->mutable_labels())["pid"] = std::to_string(getpid());
            (*m->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
            m->set_value(usage_pct);
            m->set_timestamp_ms(now_ms);
        }
    } else {
        first_proc_read = false;
    }

    prev_proc_utime = utime;
    prev_proc_stime = stime;
    last_proc_read_time = now_steady;
}

void NativeCollector::collect_memory_metrics(metrics::DynamicMetricBatch &batch, int64_t now_ms, bool collect_global) {
    std::string line, key, unit;
    double val_kb;

    double mem_total_kb = 0;
    double mem_avail_kb = 0;

    std::ifstream mem_file("/proc/meminfo");
    if (mem_file.is_open()) {
        while (std::getline(mem_file, line)) {
            std::stringstream ss(line);
            ss >> key >> val_kb >> unit;

            if (key == "MemTotal:") mem_total_kb = val_kb;
            else if (key == "MemAvailable:") mem_avail_kb = val_kb;

            if (mem_total_kb > 0 && mem_avail_kb > 0) break;
        }
    } else {
        std::cerr << "[NativeCollector] Error: Cannot open /proc/meminfo" << std::endl;
    }


    auto *m_avail = batch.add_metrics();
    m_avail->set_name("memory_available_bytes");
    (*m_avail->mutable_labels())["scope"] = "host";
    (*m_avail->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
    m_avail->set_value(mem_avail_kb * 1024.0);
    m_avail->set_timestamp_ms(now_ms);


    if (collect_global && mem_total_kb > 0) {
        double mem_used_kb = mem_total_kb - mem_avail_kb;
        double usage_pct = (mem_used_kb / mem_total_kb) * 100.0;

        auto *m_pct = batch.add_metrics();
        m_pct->set_name("memory_usage_percent");
        (*m_pct->mutable_labels())["scope"] = "global";
        (*m_pct->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
        m_pct->set_value(usage_pct);
        m_pct->set_timestamp_ms(now_ms);

        auto *m_bytes = batch.add_metrics();
        m_bytes->set_name("memory_used_bytes");
        (*m_bytes->mutable_labels())["scope"] = "global";
        (*m_bytes->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
        m_bytes->set_value(mem_used_kb * 1024.0);
        m_bytes->set_timestamp_ms(now_ms);
    }

    std::ifstream self_status("/proc/self/status");
    if (!self_status.is_open()) {
        std::cerr << "[NativeCollector] Error: Cannot open /proc/self/status" << std::endl;
        return;
    }

    double vm_rss_kb = 0;
    while (std::getline(self_status, line)) {
        std::stringstream ss(line);
        ss >> key >> val_kb >> unit;

        if (key == "VmRSS:") { // Resident Set Size (Physical RAM)
            vm_rss_kb = val_kb;
            break;
        }
    }

    if (vm_rss_kb > 0) {
        // Absolute Bytes
        auto *m_proc = batch.add_metrics();
        m_proc->set_name("memory_used_bytes");
        (*m_proc->mutable_labels())["scope"] = "process";
        (*m_proc->mutable_labels())["pid"] = std::to_string(getpid());
        (*m_proc->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
        m_proc->set_value(vm_rss_kb * 1024.0);
        m_proc->set_timestamp_ms(now_ms);

        if (mem_total_kb > 0) {
            auto *m_proc_pct = batch.add_metrics();
            m_proc_pct->set_name("memory_usage_percent");
            (*m_proc_pct->mutable_labels())["scope"] = "process";
            (*m_proc_pct->mutable_labels())["pid"] = std::to_string(getpid());
            (*m_proc_pct->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
            m_proc_pct->set_value((vm_rss_kb / mem_total_kb) * 100.0);
            m_proc_pct->set_timestamp_ms(now_ms);
        }
    }
}

void NativeCollector::collect_network_metrics(metrics::DynamicMetricBatch &batch, int64_t now_ms) {
    std::ifstream f("/proc/net/dev");
    if (!f.is_open()) {
        std::cerr << "[NativeCollector] Error: Cannot open /proc/net/dev" << std::endl;
        return;
    }

    // skip headers
    std::string line;
    std::getline(f, line);
    std::getline(f, line);

    auto now_steady = std::chrono::steady_clock::now();
    double duration_sec = 0.0;

    if (!first_net_read) {
        duration_sec = std::chrono::duration<double>(now_steady - last_net_read_time).count();
    }

    while (std::getline(f, line)) {
        size_t colon_pos = line.find(':');
        if (colon_pos == std::string::npos) continue;

        std::string iface_name = trim(line.substr(0, colon_pos));

        // ignore loopback since irrelevant
        if (iface_name == "lo") continue;

        std::stringstream ss(line.substr(colon_pos + 1));
        unsigned long long rx_bytes, tx_bytes;
        unsigned long long dummy; // for packages, errors, etc.

        // Fields: RX_bytes(1) packets(2) errs(3) drop(4) fifo(5) frame(6) compressed(7) multicast(8)
        //         TX_bytes(9) packets(10) ...
        ss >> rx_bytes; // 1
        for (int i = 0; i < 7; i++) ss >> dummy; // skip 2-8
        ss >> tx_bytes;

        if (!first_net_read && duration_sec > 0) {
            NetDevStats &prev = prev_net_stats[iface_name];

            if (rx_bytes >= prev.rx_bytes && tx_bytes >= prev.tx_bytes) {
                double rx_speed = (double) (rx_bytes - prev.rx_bytes) / duration_sec;
                double tx_speed = (double) (tx_bytes - prev.tx_bytes) / duration_sec;

                auto *m_rx = batch.add_metrics();
                m_rx->set_name("network_receive_bytes_per_sec");
                (*m_rx->mutable_labels())["interface"] = iface_name;
                (*m_rx->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
                m_rx->set_value(rx_speed);
                m_rx->set_timestamp_ms(now_ms);

                auto *m_tx = batch.add_metrics();
                m_tx->set_name("network_transmit_bytes_per_sec");
                (*m_tx->mutable_labels())["interface"] = iface_name;
                (*m_tx->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
                m_tx->set_value(tx_speed);
                m_tx->set_timestamp_ms(now_ms);
            }
        }

        prev_net_stats[iface_name] = {rx_bytes, tx_bytes};
    }

    last_net_read_time = now_steady;
    first_net_read = false;
}

void NativeCollector::collect_infiniband_metrics(metrics::DynamicMetricBatch &batch, int64_t now_ms) {
    std::string base_path = "/sys/class/infiniband";
    static bool logged_ib_error = false;

    if (!fs::exists(base_path)) {
        if (!logged_ib_error) {
            std::cerr << "[NativeCollector] Error: Infiniband base path does not exist! Suppressing further warnings." << std::endl;
            logged_ib_error = true;
        }
        return;
    }

    auto now_steady = std::chrono::steady_clock::now();
    double duration_sec = 0.0;

    if (!first_ib_read) {
        duration_sec = std::chrono::duration<double>(now_steady - last_ib_read_time).count();
    }

    // iterate through all IB devices (e.g. mlx5_0, mlx5_1, ...)
    for (const auto &entry: fs::directory_iterator(base_path)) {
        if (!entry.is_directory()) continue;

        std::string device_name = entry.path().filename().string();
        fs::path ports_dir = entry.path() / "ports";

        if (!fs::exists(ports_dir)) continue;

        // iterate through all ports
        for (const auto &port_entry: fs::directory_iterator(ports_dir)) {
            std::string port_num = port_entry.path().filename().string();
            fs::path counters_dir = port_entry.path() / "counters"; // might also be "counters_ext" for some drivers

            if (!fs::exists(counters_dir)) continue;

            unsigned long long xmit_data = read_sysfs_ull(counters_dir / "port_xmit_data");
            unsigned long long rcv_data = read_sysfs_ull(counters_dir / "port_rcv_data");
            unsigned long long xmit_pkts = read_sysfs_ull(counters_dir / "port_xmit_packets");
            unsigned long long rcv_pkts = read_sysfs_ull(counters_dir / "port_rcv_packets");

            std::string key = device_name + ":" + port_num;

            if (!first_ib_read && duration_sec > 0) {
                IBPortStats &prev = prev_ib_stats[key];

                if (xmit_data >= prev.xmit_data && rcv_data >= prev.rcv_data) {
                    // IB counter are usually 4-Byte words
                    // x4 for actual bytes
                    double tx_bytes_sec = (double) (xmit_data - prev.xmit_data) * 4.0 / duration_sec;
                    double rx_bytes_sec = (double) (rcv_data - prev.rcv_data) * 4.0 / duration_sec;
                    double tx_pkts_sec = (double) (xmit_pkts - prev.xmit_pkts) / duration_sec;
                    double rx_pkts_sec = (double) (rcv_pkts - prev.rcv_pkts) / duration_sec;

                    auto *m_tx = batch.add_metrics();
                    m_tx->set_name("infiniband_transmit_bytes_per_sec");
                    (*m_tx->mutable_labels())["device"] = device_name;
                    (*m_tx->mutable_labels())["port"] = port_num;
                    (*m_tx->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
                    m_tx->set_value(tx_bytes_sec);
                    m_tx->set_timestamp_ms(now_ms);

                    auto *m_rx = batch.add_metrics();
                    m_rx->set_name("infiniband_receive_bytes_per_sec");
                    (*m_rx->mutable_labels())["device"] = device_name;
                    (*m_rx->mutable_labels())["port"] = port_num;
                    (*m_rx->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
                    m_rx->set_value(rx_bytes_sec);
                    m_rx->set_timestamp_ms(now_ms);

                    auto *m_tx_p = batch.add_metrics();
                    m_tx_p->set_name("infiniband_transmit_packets_per_sec");
                    (*m_tx_p->mutable_labels())["device"] = device_name;
                    (*m_tx_p->mutable_labels())["port"] = port_num;
                    (*m_tx_p->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
                    m_tx_p->set_value(tx_pkts_sec);
                    m_tx_p->set_timestamp_ms(now_ms);

                    auto *m_rx_p = batch.add_metrics();
                    m_rx_p->set_name("infiniband_receive_packets_per_sec");
                    (*m_rx_p->mutable_labels())["device"] = device_name;
                    (*m_rx_p->mutable_labels())["port"] = port_num;
                    (*m_rx_p->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
                    m_rx_p->set_value(rx_pkts_sec);
                    m_rx_p->set_timestamp_ms(now_ms);
                }
            }
            prev_ib_stats[key] = {xmit_data, rcv_data, xmit_pkts, rcv_pkts};
        }
    }
    last_ib_read_time = now_steady;
    first_ib_read = false;
}

void NativeCollector::collect_app_metrics(metrics::DynamicMetricBatch &batch, int64_t now_ms) {
    size_t size = app_callbacks.get_intermediate_table_size();
    auto *m = batch.add_metrics();
    m->set_name("intermediate_table_size_bytes");
    (*m->mutable_labels())["scope"] = "application";
    (*m->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
    m->set_value(static_cast<double>(size));
    m->set_timestamp_ms(now_ms);


    size_t cols = app_callbacks.get_column_count();
    auto *x = batch.add_metrics();
    x->set_name("catalog_column_count");
    (*x->mutable_labels())["scope"] = "application";
    (*x->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
    x->set_value(static_cast<double>(cols));
    x->set_timestamp_ms(now_ms);


    size_t cat_size = app_callbacks.get_total_catalog_size();
    auto *z = batch.add_metrics();
    z->set_name("catalog_total_size_bytes");
    (*z->mutable_labels())["scope"] = "application";
    (*z->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
    z->set_value(static_cast<double>(cat_size));
    z->set_timestamp_ms(now_ms);
}

void NativeCollector::collect_process_io(metrics::DynamicMetricBatch &batch, int64_t now_ms) {
    std::ifstream f("/proc/self/io");
    if (!f.is_open()) return;

    std::string line, key;
    unsigned long long val;
    IOStats curr;

    while (std::getline(f, line)) {
        std::stringstream ss(line);
        ss >> key >> val;
        // rchar: bytes read from any fd (disk, net, pipe)
        if (key == "rchar:") curr.rchar = val;
        // wchar: bytes written to any fd
        if (key == "wchar:") curr.wchar = val;
    }

    auto now_steady = std::chrono::steady_clock::now();

    if (!first_io_read) {
        double duration_sec = std::chrono::duration<double>(now_steady - last_io_read_time).count();
        if (duration_sec > 0) {
            double read_speed = (double)(curr.rchar - prev_io_stats.rchar) / duration_sec;
            double write_speed = (double)(curr.wchar - prev_io_stats.wchar) / duration_sec;

            auto *m_r = batch.add_metrics();
            m_r->set_name("process_io_read_bytes_per_sec");
            (*m_r->mutable_labels())["scope"] = "process";
            (*m_r->mutable_labels())["pid"] = std::to_string(getpid());
            (*m_r->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
            m_r->set_value(read_speed);
            m_r->set_timestamp_ms(now_ms);

            auto *m_w = batch.add_metrics();
            m_w->set_name("process_io_write_bytes_per_sec");
            (*m_w->mutable_labels())["scope"] = "process";
            (*m_w->mutable_labels())["pid"] = std::to_string(getpid());
            (*m_w->mutable_labels())["host_machine_uuid"] = host_machine_uuid_string;
            m_w->set_value(write_speed);
            m_w->set_timestamp_ms(now_ms);
        }
    } else {
        first_io_read = false;
    }

    prev_io_stats = curr;
    last_io_read_time = now_steady;
}

std::string NativeCollector::exec_cmd(const char *cmd) {
    FILE *pipe = popen(cmd, "r");
    if (!pipe) return "";
    char buffer[256];
    std::string result;
    while (fgets(buffer, sizeof(buffer), pipe)) result += buffer;
    pclose(pipe);
    return result;
}

void NativeCollector::collect_static_info() {
    collect_cpu_detailed();
    collect_cpu();
    collect_mem_detailed();
    collect_mem();
    collect_numa();
    collect_pci();
    collect_kernel();
    collect_host_machine_uuid();
}

void NativeCollector::collect_cpu_detailed() {
    std::ifstream f("/proc/cpuinfo");
    if (!f.is_open()) return;
    std::stringstream buffer;
    buffer << f.rdbuf();
    parse_cpu_detailed(buffer.str(), this->cpu_info_detailed_pb);
}

void NativeCollector::collect_cpu() {
    std::string raw = exec_cmd("lscpu");
    parse_key_value_lines(raw, this->cpu_info_pb.mutable_fields());
}

void NativeCollector::collect_mem_detailed() {
    std::ifstream f("/proc/meminfo");
    if (!f.is_open()) return;
    std::stringstream buffer;
    buffer << f.rdbuf();
    parse_mem_detailed(buffer.str(), this->mem_info_detailed_pb);
}

void NativeCollector::collect_mem() {
    std::string raw = exec_cmd("lsmem");
    parse_lines_to_repeated(raw, this->mem_info_pb);
}

void NativeCollector::collect_numa() {
    std::string raw = exec_cmd("numactl --hardware");
    parse_lines_to_repeated(raw, this->numa_info_pb);
}

void NativeCollector::collect_pci() {
    std::string raw = exec_cmd("lspci");
    parse_lines_to_repeated(raw, this->pci_info_pb);
}

void NativeCollector::collect_kernel() {
    struct utsname buffer;
    if (uname(&buffer) != 0) return;

    std::string raw = buffer.release;
    int major = 0, minor = 0, patch = 0, build = 0;
    int items = sscanf(raw.c_str(), "%d.%d.%d-%d", &major, &minor, &patch, &build);

    if (items < 4) {
        build = 0;
        if (items < 3) {
            sscanf(raw.c_str(), "%d.%d.%d", &major, &minor, &patch);
        }
    }

    kernel_info_pb.set_major(major);
    kernel_info_pb.set_minor(minor);
    kernel_info_pb.set_patch(patch);
    kernel_info_pb.set_build_number(build);
    kernel_info_pb.set_full_release_string(raw);
}

void NativeCollector::collect_host_machine_uuid() {
    std::ifstream f("/etc/machine-id");
    if (!f.is_open()) return;
    std::string uuid;
    f >> uuid;
    host_machine_uuid_pb.set_uuid(uuid);
    host_machine_uuid_string = uuid;
}

#include <iostream>
#include <infrastructure/MonitorClient.hpp>
#include <Utility.hpp>
#include <UnitDefinition.pb.h>
#include <thread>


using namespace tuddbs;

std::string unitTypeToString(UnitType type) {
    switch (type) {
        case UnitType::UNDEFINED_UNIT_TYPE: return "Undefined";
        case UnitType::QUERY_PLANER: return "QueryPlaner";
        case UnitType::COMPUTE_UNIT: return "ComputeUnit";
        case UnitType::MEMORY_UNIT: return "MemoryUnit";
        case UnitType::META_UNIT: return "MetaUnit";
        case UnitType::MONITOR_UNIT: return "Monitor";
        case UnitType::DATABASE_UNIT: return "Server";
        default: return "Unknown";
    }
}

metrics::HostUnitType create_host_unit_type_pb(UnitType type) {
    metrics::HostUnitType host_unit_type;
    host_unit_type.set_type(unitTypeToString(type));
    return host_unit_type;
}


MonitorClient::MonitorClient(const std::string &ip,
                             const size_t port,
                             const uint64_t host_unit_uuid,
                             UnitType host_type,
                             const size_t monitor_interval,
                             size_t accumulation_count,
                             AppMetricCallbacks app_callbacks)
    : client(ip, port),
      host_unit_uuid(host_unit_uuid),
      host_type(host_type),
      monitor_interval_ms(monitor_interval),
      accumulation_limit(accumulation_count),
      collector(app_callbacks) {


    auto updateUnitInfo_cb = [this](TCPMetaInfo *meta, void *data, size_t len) -> void {
        std::cout << "[UpdateUnitInfo Callback] Invoked." << std::endl;

        UnitDefinition unit;
        unit.set_unit_type(static_cast<uint32_t>(UnitType::MONITOR_UNIT));

        TCPMetaInfo info;
        info.package_type = TcpPackageType::UPDATE_UNIT_TYPE;
        info.payload_size = unit.ByteSizeLong();
        info.src_uuid = client.getUuid();

        std::vector<unsigned char> buf(sizeof(TCPMetaInfo) + info.payload_size);
        Utility::serializeItemToMemory(buf.data(), unit, info);
        client.notifyHost(buf.data(), buf.size());
        this->metric_thread = std::thread([this]() {
            this->send_all_static_info();
            this->run_metric_loop();
        });
    };


    client.addCallback(TcpPackageType::UPDATE_UNIT_TYPE, updateUnitInfo_cb);
    client.start();
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

MonitorClient::~MonitorClient() {
    running = false;
    if (metric_thread.joinable()) metric_thread.join();
}

std::string MonitorClient::generate_metric_signature(const metrics::DynamicMetric& m) {
    // syntax: "name|label1=val1|label2=val2|..."
    std::stringstream ss;
    ss << m.name();
    for (auto const& [key, val] : m.labels()) {
        ss << "|" << key << "=" << val;
    }
    return ss.str();
}

void MonitorClient::send_all_static_info() {
    send_compressed_message(collector.get_host_machine_uuid(), TcpPackageType::HostMachineUuid);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    send_compressed_message(create_host_unit_type_pb(host_type), TcpPackageType::HostUnitType);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    send_compressed_message(collector.get_cpu_info_detailed(), TcpPackageType::CpuInfoDetailed);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    send_compressed_message(collector.get_cpu_info(), TcpPackageType::CpuInfo);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    send_compressed_message(collector.get_mem_info_detailed(), TcpPackageType::MemInfoDetailed);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    send_compressed_message(collector.get_mem_info(), TcpPackageType::MemInfo);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    send_compressed_message(collector.get_numa_info(), TcpPackageType::NumaInfo);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    send_compressed_message(collector.get_pci_info(), TcpPackageType::PciInfo);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    send_compressed_message(collector.get_kernel_info(), TcpPackageType::KernelInfo);
}

void MonitorClient::run_metric_loop() {
    while (running) {
        bool am_i_leader = collector.try_acquire_leadership();
        metrics::DynamicMetricBatch current_batch = collector.collect_dynamic_metrics(am_i_leader);

        if (accumulation_limit <= 1) {
            // --- STANDARD MODE ---
            if (current_batch.metrics_size() > 0) {
                send_compressed_message(current_batch, TcpPackageType::DynamicMetricBatch);
            }
        } else {
            // --- SLIM / ACCUMULATION MODE ---
            for (const auto& metric : current_batch.metrics()) {
                std::string sig = generate_metric_signature(metric);
                if (metric_buffer.find(sig) == metric_buffer.end()) {
                    MetricSeries series;
                    series.name = metric.name();
                    for(const auto& [k, v] : metric.labels()) {
                        series.labels[k] = v;
                    }
                    metric_buffer[sig] = series;
                }
                metric_buffer[sig].values.push_back(metric.value());
            }

            current_accumulation_count++;
            if (current_accumulation_count >= accumulation_limit) {
                flush_accumulated_metrics();
                metric_buffer.clear();
                current_accumulation_count = 0;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(monitor_interval_ms));
    }
}

void MonitorClient::flush_accumulated_metrics() {
    if (metric_buffer.empty()) return;

    metrics::DynamicMetricBatch out_batch;
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    for (auto& [sig, series] : metric_buffer) {
        std::vector<double>& vals = series.values;
        if (vals.empty()) continue;

        std::sort(vals.begin(), vals.end());

        double min_val = vals.front();
        double max_val = vals.back();
        double sum = std::accumulate(vals.begin(), vals.end(), 0.0);
        double avg_val = sum / vals.size();
        double median_val = 0.0;

        if (vals.size() % 2 == 0) {
            median_val = (vals[vals.size()/2 - 1] + vals[vals.size()/2]) / 2.0;
        } else {
            median_val = vals[vals.size()/2];
        }

        auto add_stat_metric = [&](const std::string& suffix, double val) {
            auto* m = out_batch.add_metrics();
            m->set_name(series.name + suffix);
            m->set_value(val);
            m->set_timestamp_ms(now);
            for (const auto& [l_key, l_val] : series.labels) {
                (*m->mutable_labels())[l_key] = l_val;
            }
        };

        add_stat_metric("_min", min_val);
        add_stat_metric("_max", max_val);
        add_stat_metric("_avg", avg_val);
        add_stat_metric("_median", median_val);
    }

    if (out_batch.metrics_size() > 0) {
        send_compressed_message(out_batch, TcpPackageType::DynamicMetricBatch);
    }
}

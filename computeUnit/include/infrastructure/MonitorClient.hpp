#ifndef DISAGGDBMONITOR_MONITORCLIENT_H
#define DISAGGDBMONITOR_MONITORCLIENT_H


#include <fstream>
#include <mutex>

#include "Utility.hpp"
#include <infrastructure/NativeCollector.hpp>
#include "TCPClient.hpp"
#include <vector>

#include <snappy.h>

#include <StaticSysMetric.pb.h>

class MonitorClient {
private:

    size_t monitor_interval_ms; // (size_t cannot be negative)

    // SLIM MODE CONFIG
    size_t accumulation_limit = 1; // 1 = send directly
    size_t current_accumulation_count = 0;

    struct MetricSeries {
        std::string name;
        std::map<std::string, std::string> labels;
        std::vector<double> values;
    };

    std::map<std::string, MetricSeries> metric_buffer;

    std::atomic<bool> running{true};
    std::thread metric_thread;

    tuddbs::TCPClient client;
    uint64_t host_unit_uuid;
    UnitType host_type;

    NativeCollector collector;

    void send_all_static_info();

    void run_metric_loop();
    std::string generate_metric_signature(const metrics::DynamicMetric& m);
    void flush_accumulated_metrics();

public:
    MonitorClient(const std::string &ip,
                  size_t port,
                  uint64_t host_unit_uuid,
                  UnitType host_type,
                  size_t monitor_interval,
                  size_t accumulation_count = 1,
                  AppMetricCallbacks app_callbacks = AppMetricCallbacks());

    ~MonitorClient();

    template<typename ProtoMessage>
    void send_compressed_message(const ProtoMessage &pb_message, TcpPackageType pkg_type);
};

template<typename ProtoMessage>
void MonitorClient::send_compressed_message(const ProtoMessage &pb_message, TcpPackageType pkg_type) {
    std::string serialized_proto;
    if (!pb_message.SerializeToString(&serialized_proto)) {
        std::cerr << "[Error] Failed to serialize protobuf message." << std::endl;
        return;
    }

    std::string compressed_payload;
    snappy::Compress(serialized_proto.data(), serialized_proto.size(), &compressed_payload);

    tuddbs::TCPMetaInfo meta;
    meta.package_type = pkg_type;
    meta.payload_size = compressed_payload.size();
    meta.src_uuid = client.getUuid();
    meta.host_unit_uuid = this->host_unit_uuid;
    meta.unit_type = this->host_type;

    std::vector<unsigned char> buffer(sizeof(tuddbs::TCPMetaInfo) + meta.payload_size);
    std::memcpy(buffer.data(), &meta, sizeof(tuddbs::TCPMetaInfo));
    if (meta.payload_size > 0) {
        std::memcpy(buffer.data() + sizeof(tuddbs::TCPMetaInfo),
                    compressed_payload.data(),
                    compressed_payload.size());
    }

    client.notifyHost(buffer.data(), buffer.size());
}

#endif

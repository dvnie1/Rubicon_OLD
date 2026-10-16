#include <functional>
#include <iostream>
#include <sstream>
#include <string>

#include "include/infrastructure/ArgParser.hpp"
#include "TCPServer.hpp"
#include "Utility.hpp"

#include <google/protobuf/util/json_util.h>
#include "httplib.h"
#include "nlohmann/json.hpp"
#include <snappy.h>

#include "NetworkRequests.pb.h"
#include "WorkItem.pb.h"
#include "WorkResponse.pb.h"
#include <StaticSysMetric.pb.h>
#include "remote.pb.h"
#include "types.pb.h"

using json = nlohmann::json;

const std::string PROMETHEUS_WRITE_PATH = "/api/v1/write";

using namespace tuddbs;

std::unordered_map<int, std::string> socket_to_uuid_map;
std::mutex map_mutex;

void globalExit(TCPServer &server) {
    server.closeConnection();
    exit(0);
}

std::string format_timestamp(int64_t ts_ms) {
    auto seconds = std::chrono::seconds(ts_ms / 1000);
    auto tp = std::chrono::time_point<std::chrono::system_clock>(seconds);
    auto in_time_t = std::chrono::system_clock::to_time_t(tp);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

std::string level_to_string(metrics::LogLevel level) {
    switch(level) {
        case metrics::DEBUG1: return "DEBUG1";
        case metrics::DEBUG2: return "DEBUG2";
        case metrics::INFO:  return "INFO";
        case metrics::WARNING:  return "WARNING";
        case metrics::ERROR: return "ERROR";
        case metrics::FATAL: return "FATAL";
        case metrics::NOFORMAT: return "NOFORMAT";
        case metrics::CONSOLE: return "CONSOLE";
        case metrics::SUCCESS: return "SUCCESS";
        default: return "UNKNOWN";
    }
}

std::string ProtoMessageToJson(const google::protobuf::Message &message) {
    std::string json_output;
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = false;
    //options.always_print_primitive_fields = true;

    google::protobuf::util::MessageToJsonString(message, &json_output, options);
    return json_output;
}

void update_active_status(httplib::Client &client, const std::string &host_unit_uuid, bool is_active,
                          const std::string &couch_db_name) {
    if (host_unit_uuid.empty()) return;

    std::cout << "Updating active status for host unit: " << host_unit_uuid << std::endl;

    std::string doc_path = "/" + couch_db_name + "/" + host_unit_uuid;
    auto res = client.Get(doc_path.c_str());

    json doc;
    if (res && res->status == 200) {
        doc = json::parse(res->body);
    } else if (res && res->status == 404) {
        doc["_id"] = host_unit_uuid;
    } else {
        std::cerr << "CouchDB GET Error during status update: " << (res ? res->status : -1) << std::endl;
        return;
    }

    doc["active"] = is_active;
    doc["last_seen"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    auto put_res = client.Put(doc_path.c_str(), doc.dump(), "application/json");
    if (!put_res || (put_res->status != 201 && put_res->status != 202)) {
        std::cerr << "CouchDB PUT Error (Status Update): " << (put_res ? put_res->status : -1) << std::endl;
    } else {
        std::cout << "[MonitorServer] Unit " << host_unit_uuid << " is now " << (is_active ? "ONLINE" : "OFFLINE") << std::endl;
    }
}

bool update_host_unit_document(httplib::Client &client,
                               const std::string &host_unit_uuid,
                               const std::string &metric_field_name,
                               const std::string &metric_json_string,
                               const std::string &couch_db_name) {
    std::cout << "Updating document for host unit: " << host_unit_uuid << std::endl;
    std::string doc_path = "/" + couch_db_name + "/" + host_unit_uuid;
    auto res = client.Get(doc_path.c_str());

    json doc;

    if (res && res->status == 200) {
        doc = json::parse(res->body);
    } else if (res && res->status == 404) {
        doc["_id"] = host_unit_uuid;
    } else {
        std::cerr << "CouchDB GET Error: " << (res ? res->status : -1) << " host unit: " << host_unit_uuid << std::endl;
        return false;
    }

    doc[metric_field_name] = json::parse(metric_json_string);
    doc["active"] = true;

    auto put_res = client.Put(doc_path.c_str(), doc.dump(), "application/json");
    if (put_res && (put_res->status == 201 || put_res->status == 202)) {
        std::cout << "Document updated successfully for host unit: " << host_unit_uuid << std::endl;
        return true;
    }
    std::cerr << "CouchDB PUT Error: " << (put_res ? put_res->status : -1) << " host unit: " << host_unit_uuid << std::endl;
    return false;
}

template<typename ProtoMsg>
auto create_metric_cb(httplib::Client &client, const std::string &db_name, const std::string &doc_key) {
    return [&client, db_name, doc_key](TCPMetaInfo *meta, void *data, size_t len) {

        int socket_fd = TCPServer::getCurrentThreadSocket();
        if (socket_fd != -1) {
            std::lock_guard<std::mutex> lock(map_mutex);
            socket_to_uuid_map[socket_fd] = std::to_string(meta->host_unit_uuid);
        }

        std::string uncompressed;
        if (!snappy::Uncompress((const char *) data, len, &uncompressed)) {
            std::cerr << "Snappy Decompression failed!" << std::endl;
            return;
        }

        ProtoMsg metric;
        if (metric.ParseFromString(uncompressed)) {
            std::string json_data = ProtoMessageToJson(metric);
            update_host_unit_document(client, std::to_string(meta->host_unit_uuid), doc_key, json_data, db_name);
        } else {
            std::cerr << "Failed to parse payload for " << doc_key << std::endl;
        }
    };
}

int main(int argc, char *argv[]) {
    ArgParser parser(argc, argv);

    const std::string &port_string = parser.takeParseArg<std::string>("-port", "[Info] No Port given. I am listening on port 31338 by defualt.", "31338", false);
    const std::string &couch_address = parser.takeParseArg<std::string>("-couch_address", "[Info] No CouchDB address provided. Static metrics won't be stored!", "", false);
    const std::string &couch_db_name = parser.takeParseArg<std::string>("-couch_db_name", "[Info] No CouchDB name provided. Writes to CouchDB will fail!]", "", false);
    const std::string &prometheus_address = parser.takeParseArg<std::string>("-prom_address", "[Info] No Prometheus address provided. Dynamic metrics won't be stored!", "", false);

    TCPServer server(atol(port_string.c_str()));

    httplib::Client couchClient(couch_address);
    if (auto res = couchClient.Get("/"); !res || res->status != 200) {
        std::cerr << "Wasn't able to set up CouchDB client!" << std::endl;
    }
    couchClient.set_basic_auth("admin", "secret");

    httplib::Client promClient(prometheus_address);
    if (auto res2 = promClient.Get("/"); !res2 || res2->status != 200) {
        std::cerr << "Wasn't able to set up Prometheus client!" << std::endl;
    }

    std::function<void(int)> disconnect_cb = [&](int socket_fd) {
        std::string uuid_to_deactivate;
        {
            std::lock_guard<std::mutex> lock(map_mutex);
            if (socket_to_uuid_map.find(socket_fd) != socket_to_uuid_map.end()) {
                uuid_to_deactivate = socket_to_uuid_map[socket_fd];
                socket_to_uuid_map.erase(socket_fd);
            }
        }
        if (!uuid_to_deactivate.empty()) {
            update_active_status(couchClient, uuid_to_deactivate, false, couch_db_name);
        }
    };
    server.setDisconnectCallback(disconnect_cb);

    auto host_machine_uuid_cb =     create_metric_cb<metrics::HostMachineUuid>(couchClient, couch_db_name, "host_machine_uuid");
    auto host_unit_type_cb =        create_metric_cb<metrics::HostUnitType>(couchClient, couch_db_name, "host_unit_type");
    auto cpu_info_detailed_cb =     create_metric_cb<metrics::CpuInfoDetailed>(couchClient, couch_db_name, "cpu_info_detailed");
    auto cpu_info_cb =              create_metric_cb<metrics::CpuInfo>(couchClient, couch_db_name, "cpu_info");
    auto mem_info_detailed_cb =     create_metric_cb<metrics::MemInfoDetailed>(couchClient, couch_db_name, "mem_info_detailed");
    auto mem_info_cb =              create_metric_cb<metrics::MemInfo>(couchClient, couch_db_name, "mem_info");
    auto numa_info_cb =             create_metric_cb<metrics::NumaInfo>(couchClient, couch_db_name, "numa_info");
    auto pci_info_cb =              create_metric_cb<metrics::PciInfo>(couchClient, couch_db_name, "pci_info");
    auto kernel_info_cb =           create_metric_cb<metrics::KernelInfo>(couchClient, couch_db_name, "kernel_info");

    auto prometheus_forward_cb = [&promClient, &server](TCPMetaInfo *meta, void *data, size_t len) -> void {

        int socket_fd = TCPServer::getCurrentThreadSocket();
        if (socket_fd != -1) {
            std::lock_guard<std::mutex> lock(map_mutex);
            socket_to_uuid_map[socket_fd] = std::to_string(meta->host_unit_uuid);
        }
        std::string uncompressed_data;
        if (!snappy::Uncompress((const char *) data, len, &uncompressed_data)) {
            std::cerr << "[Error] Snappy Decompression failed!" << std::endl;
            return;
        }
        metrics::DynamicMetricBatch my_batch;
        if (!my_batch.ParseFromString(uncompressed_data)) {
            std::cerr << "Error parsing DynamicMetricBatch" << std::endl;
            return;
        }

        prometheus::WriteRequest req;

        for (const auto &my_metric: my_batch.metrics()) {
            auto *ts = req.add_timeseries();

            auto *lbl_name = ts->add_labels();
            lbl_name->set_name("__name__");
            lbl_name->set_value(my_metric.name());

            auto *lbl_host = ts->add_labels();
            lbl_host->set_name("host_unit_uuid");
            lbl_host->set_value(std::to_string(meta->host_unit_uuid));

            for (auto const &[key, val]: my_metric.labels()) {
                auto *l = ts->add_labels();
                l->set_name(key);
                l->set_value(val);
            }

            auto *sample = ts->add_samples();
            sample->set_value(my_metric.value());
            sample->set_timestamp(my_metric.timestamp_ms());
        }

        std::string proto_output;
        req.SerializeToString(&proto_output);

        std::string compressed_output;
        snappy::Compress(proto_output.data(), proto_output.size(), &compressed_output);

        auto res = promClient.Post(PROMETHEUS_WRITE_PATH, compressed_output, "application/x-protobuf");

        if (res && res->status >= 200 && res->status < 300) {
            std::cout << "Pushed " << my_batch.metrics_size() << " metrics to Prometheus." << std::endl;
        } else {
            std::cerr << "Prometheus Write Error: " << (res ? res->status : -1) << std::endl;
            if (res) std::cerr << res->body << std::endl;
        }
    };

    server.addCallback(TcpPackageType::HostMachineUuid, host_machine_uuid_cb);
    server.addCallback(TcpPackageType::HostUnitType, host_unit_type_cb);
    server.addCallback(TcpPackageType::CpuInfoDetailed, cpu_info_detailed_cb);
    server.addCallback(TcpPackageType::CpuInfo, cpu_info_cb);
    server.addCallback(TcpPackageType::MemInfoDetailed, mem_info_detailed_cb);
    server.addCallback(TcpPackageType::MemInfo, mem_info_cb);
    server.addCallback(TcpPackageType::NumaInfo, numa_info_cb);
    server.addCallback(TcpPackageType::PciInfo, pci_info_cb);
    server.addCallback(TcpPackageType::KernelInfo, kernel_info_cb);
    server.addCallback(TcpPackageType::DynamicMetricBatch, prometheus_forward_cb);

    std::string content;
    std::string op;

    server.start();

    std::cout << "Monitor Server is running on port " << port_string << "." << std::endl;
    std::cout << "Press [ENTER] to stop the server." << std::endl;

    std::cin.get();

    globalExit(server);
    return 0;
}
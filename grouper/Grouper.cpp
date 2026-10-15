#include <functional>
#include <iostream>
#include <shared_mutex>
#include <sstream>
#include <string>

#include "ArgParser.hpp"
#include "DAGCollectionManager.hpp"
#include "Logger.hpp"
#include "NetworkRequests.pb.h"
#include "PlanDAG.hpp"
#include "QueryPlan.pb.h"
#include "TCPServer.hpp"
#include "Utility.hpp"
#include "WorkItem.pb.h"
#include "WorkRequest.pb.h"
#include "WorkResponse.pb.h"

#include "AdaptiveLoadBalancer.hpp"

using namespace tuddbs;

void globalExit(TCPServer& server) {
    server.closeConnection();
    exit(0);
}

int main(int argc, char* argv[]) {
    ArgParser parser(argc, argv);

    const std::string& port_string = parser.takeParseArg<std::string>("-port", "[Info] No Port given. I am listening on port 23232 by default.", "23232", false);
    const bool verbose = parser.takeParseArg<bool>("-v", "", false, false);

    TCPServer server(atol(port_string.c_str()));
    lb::AdaptiveLoadBalancer loadBalancer("http://127.0.0.1:8428");

    std::shared_mutex collectionMutex;

    std::unique_ptr<DagCollectionManager> collectionManager = std::make_unique<DagCollectionManager>(
    server, loadBalancer, 10, std::chrono::milliseconds(100)
);

    auto forward_cb = [&server, &verbose](TCPMetaInfo* meta, void* data, size_t len) -> void {
        VLOG("[Forward_Cb] Invoking");

        ClientInfo* target = nullptr;
        const bool targeted = (meta->tgt_uuid != 0);
        if (targeted) {
            target = server.getClientByUuid(meta->tgt_uuid);
            if (!target) {
                std::cerr << "[TCPServer] ERROR - UUID<" << meta->tgt_uuid << "> not found. Could not send to destination." << std::endl;
                return;
            }
        }

#ifdef VERBOSE_OUTPUT
        VLOG("== Forwarding Message ==");
        if (targeted) {
            VLOG("--       Target: " << target->prettyName);
        } else {
            VLOG("--       Target: _random_");
        }
        VLOG("-- Pacakge type: " << TcpPackageType_Name(meta->package_type));
        VLOG("-- message_size: " << sizeof(TCPMetaInfo) + len);
#endif

        const size_t message_size = sizeof(TCPMetaInfo) + len;
        void* buf = malloc(message_size);

        memcpy(buf, meta, sizeof(TCPMetaInfo));
        memcpy(reinterpret_cast<char*>(buf) + sizeof(TCPMetaInfo), data, len);

        if (targeted) {
            server.sendTo(target, reinterpret_cast<const char*>(buf), message_size);
        } else {
            server.sendToAnyOfType(UnitType::COMPUTE_UNIT, reinterpret_cast<const char*>(buf), message_size);
        }
        server.sendToAllOfType(UnitType::MONITOR_UNIT, reinterpret_cast<const char*>(buf), message_size);

        free(buf);
    };

    auto text_cb = [&server, &forward_cb](TCPMetaInfo* meta, void* data, size_t len) -> void {
        VLOG("[Text Callback] Invoked.");
        if (meta->tgt_uuid == 0) {
            std::string str(reinterpret_cast<char*>(data), len);
            VLOG(" -- Received Text: " << str);
        } else {
            forward_cb(meta, data, len);
        }
    };

    auto reroute_work_cb = [&server](TCPMetaInfo* meta, void* data, size_t len) -> void {
        VLOG("[RerouteWork] I received a message to reroute a previously issued WorkRequest.");

        const size_t message_size = sizeof(TCPMetaInfo) + len;
        void* buf = malloc(message_size);
        TCPMetaInfo message_info;
        message_info.package_type = TcpPackageType::REROUTE_WORK;
        message_info.payload_size = len;
        message_info.src_uuid = meta->tgt_uuid;
        message_info.tgt_uuid = 0;
        memcpy(buf, &message_info, sizeof(TCPMetaInfo));
        memcpy(reinterpret_cast<char*>(buf) + sizeof(TCPMetaInfo), data, len);

        server.rerouteToAnyOfType(UnitType::COMPUTE_UNIT, meta->src_uuid, static_cast<const char*>(buf), message_size);
        server.sendToAllOfType(UnitType::MONITOR_UNIT, static_cast<const char*>(buf), message_size);
    };

    auto monitor_cb = [&server](TCPMetaInfo* meta, [[maybe_unused]] void* data, [[maybe_unused]] size_t len) -> void {
        VLOG("[Monitor Cb] Invoking");

        ClientInfo* monitor = server.getClientByUuid(meta->src_uuid);
        if (monitor) {
            const std::string& monitorInfo = server.monitorInfoToString();

            TCPMetaInfo message_info;
            message_info.package_type = TcpPackageType::TEXT;
            message_info.payload_size = monitorInfo.size();
            message_info.src_uuid = meta->src_uuid;
            message_info.tgt_uuid = meta->tgt_uuid;

            void* buf = malloc(message_info.bytesize());
            memcpy(buf, &message_info, sizeof(TCPMetaInfo));
            memcpy(reinterpret_cast<char*>(buf) + sizeof(TCPMetaInfo), monitorInfo.c_str(), monitorInfo.size());
            server.sendTo(monitor, reinterpret_cast<const char*>(buf), message_info.bytesize());
            server.sendToAllOfType(UnitType::MONITOR_UNIT, reinterpret_cast<const char*>(buf), message_info.bytesize());
            free(buf);
        } else {
            VLOG("[TCPServer] ERROR - UUID<" << meta->tgt_uuid << "> not found. Could not respond to destination.");
        }
    };

    auto uuid_per_client_cb = [&server](TCPMetaInfo* meta, void* data, size_t len) -> void {
        VLOG("[Uuid Per Client Cb] Invoking");
        ddd::network::UuidForUnitRequest request;
        request.ParseFromArray(data, len);

        ddd::network::UuidForUnitResponse response;
        response.set_requestedunittype(request.requestedunittype());
        auto uuid_response = response.mutable_uuids();
        auto name_response = response.mutable_names();

        UnitType type = static_cast<UnitType>(request.requestedunittype());
        auto uuids_vec = server.getUuidForUnitType(type);
        VLOG("Uuids for type [" << UnitType_Name(type) << "]");
        for (auto it = uuids_vec.begin(); it != uuids_vec.end(); ++it) {
            VLOG(it->first << " " << it->second);
            name_response->Add(it->first.c_str());
            uuid_response->Add(it->second);
        }
        VLOG("\nResponse Item:");
#ifdef VERBOSE_OUTPUT
        response.PrintDebugString();
#endif

        tuddbs::TCPMetaInfo info;
        info.package_type = TcpPackageType::UUID_FOR_UNIT_RESPONSE;
        info.payload_size = response.ByteSizeLong();
        info.src_uuid = meta->src_uuid;
        info.tgt_uuid = meta->tgt_uuid;
        void* out_mem = malloc(info.bytesize());
        tuddbs::Utility::serializeItemToMemory(out_mem, response, info);
        ClientInfo* requester = server.getClientByUuid(meta->src_uuid);
        if (requester) {
            server.sendTo(requester, reinterpret_cast<const char*>(out_mem), info.bytesize());
            server.sendToAllOfType(UnitType::MONITOR_UNIT, reinterpret_cast<const char*>(out_mem), info.bytesize());
        } else {
            VLOG("[TCPServer] ERROR - UUID<" << meta->tgt_uuid << "> not found. Please set your UUID before requesting.");
        }
        free(out_mem);
    };

    auto query_plan_cb = [&server, &collectionManager, &collectionMutex](TCPMetaInfo* meta, void* data, size_t len) -> void {
        VLOG("[Query_Plan_Cb] I received a QueryPlan message.");

        WorkRequest request;
        request.ParseFromArray(data, len);

        QueryPlan plan = request.queryplan();

        plan::PlanDAG dag(std::move(plan), meta);
        dag.build();

        std::shared_lock lock(collectionMutex);
        collectionManager->add_dag(std::move(dag));
    };

    auto server_configuration_cb = [&server, &collectionManager, &forward_cb, &query_plan_cb, &collectionMutex, &loadBalancer](TCPMetaInfo* meta, void* data, size_t len) -> void {
        VLOG("[Server_Configuration_Cb] I received a ServerConfiguration message.");

        ddd::network::ServerConfigurationRequest request;
        request.ParseFromArray(data, len);

        bool success = true;

        if (request.has_callbackchange()) {
            switch (request.callbackchange().callbackfunction()) {
                case ddd::network::CallbackFunction::FORWARD_CB: {
                    server.addCallback(request.callbackchange().packagetype(), forward_cb);
                } break;
                case ddd::network::CallbackFunction::QUERY_PLAN_CB: {
                    server.addCallback(request.callbackchange().packagetype(), query_plan_cb);
                } break;
                default: {
                    std::cerr << "[Server_Configuration_Cb] Unknown Callback Function Type." << std::endl;
                    success = false;
                }
            }
        }

        if (request.has_threshold() || request.has_windowsizems() || request.has_maxoverhead()) {
            const uint64_t threshold = request.has_threshold() ? request.threshold() : 10;
            const auto window_size = std::chrono::milliseconds(request.has_windowsizems() ? request.windowsizems() : 100);
            const float max_overhead = request.has_maxoverhead() ? request.maxoverhead() : 2.0f;

            std::unique_lock lock(collectionMutex);
            collectionManager = std::make_unique<DagCollectionManager>(server, loadBalancer, threshold, window_size, max_overhead);
        }

        ddd::network::ServerConfigurationResponse response;
        response.set_success(success);

        tuddbs::TCPMetaInfo info;
        info.package_type = TcpPackageType::SERVER_CONFIGURATION_RESPONSE;
        info.payload_size = response.ByteSizeLong();
        info.src_uuid = meta->tgt_uuid;
        info.tgt_uuid = meta->src_uuid;
        void* out_mem = malloc(info.bytesize());
        tuddbs::Utility::serializeItemToMemory(out_mem, response, info);
        ClientInfo* requester = server.getClientByUuid(meta->src_uuid);
        if (requester) {
            server.sendTo(requester, reinterpret_cast<const char*>(out_mem), info.bytesize());
            server.sendToAllOfType(UnitType::MONITOR_UNIT, reinterpret_cast<const char*>(out_mem), info.bytesize());
        } else {
            std::cerr << "[TCPServer] ERROR - UUID<" << meta->tgt_uuid << "> not found. Please set your UUID before requesting." << std::endl;
        }
        free(out_mem);
    };

    auto plan_response_cb = [&loadBalancer, &forward_cb](TCPMetaInfo* meta, void* data, size_t len) -> void {
        PlanResponse response;
        if (response.ParseFromArray(data, len)) {
            loadBalancer.onPlanCompleted(response.planid(), response.success());
        }
        forward_cb(meta, data, len);
    };

    server.addCallback(TcpPackageType::TEXT, text_cb);
    server.addCallback(TcpPackageType::WORK, forward_cb);
    server.addCallback(TcpPackageType::QUERY_PLAN, forward_cb);
    server.addCallback(TcpPackageType::REROUTE_WORK, reroute_work_cb);
    server.addCallback(TcpPackageType::TASK_FINISHED, forward_cb);
    server.addCallback(TcpPackageType::MONITOR_REQUEST, monitor_cb);
    server.addCallback(TcpPackageType::CONNECT_ACTION, forward_cb);
    server.addCallback(TcpPackageType::CONNECT_ACTION_INFO, forward_cb);
    server.addCallback(TcpPackageType::UUID_FOR_UNIT_REQUEST, uuid_per_client_cb);
    server.addCallback(TcpPackageType::CONFIGURATION_ACTION, forward_cb);
    server.addCallback(TcpPackageType::PLAN_RESPONSE, plan_response_cb);
    server.addCallback(TcpPackageType::SERVER_CONFIGURATION, server_configuration_cb);

    std::string content;
    std::string op;

    server.start();

    bool abort = false;
    while (!abort) {
        op = "-1";
        std::cout << "Type \"exit\" to terminate." << std::endl;
        // std::cin >> op;
        std::getline(std::cin, op, '\n');
        if (op == "-1") {
            globalExit(server);
        }

        std::cout << "Chosen:" << op << std::endl;
        std::transform(op.begin(), op.end(), op.begin(), [](unsigned char c) { return std::tolower(c); });

        if (op == "exit") {
            globalExit(server);
        } else {
            std::size_t id;
            bool converted = false;
            std::cout << " ?> " << std::flush;
            try {
                id = stol(op);
                converted = true;
            } catch (...) {
                std::cout << "No number given." << std::endl;
                continue;
            }
            if (converted) {
                std::cout << "Number: " << id << std::endl;
                switch (id) {
                    case 1: {
                        std::string text = "Hi from the server.";
                        TCPMetaInfo info;
                        info.package_type = TcpPackageType::TEXT;
                        info.payload_size = text.size();
                        const size_t message_size = sizeof(TCPMetaInfo) + text.size();
                        void* buf = malloc(message_size);
                        memcpy(buf, &info, sizeof(TCPMetaInfo));
                        memcpy(static_cast<char*>(buf) + sizeof(TCPMetaInfo), text.c_str(), text.size());
                        server.sendToAll(static_cast<const char*>(buf), message_size);
                        free(buf);
                    } break;
                    case 2: {
                        const size_t itemCount = tuddbs::Utility::generateRandomNumber(1, 10);

                        std::vector<WorkItem> items;
                        size_t itemsByteSize = 0;
                        for (size_t i = 0; i < itemCount; ++i) {
                            WorkItem item = tuddbs::Utility::generateRandomWorkItem(true);
                            item.set_itemid(i);
                            itemsByteSize += item.ByteSizeLong();
                            items.push_back(item);
                        }

                        std::cout << "Serializing " << itemCount << " items..." << std::endl;

                        const size_t message_size = itemsByteSize + (sizeof(TCPMetaInfo) * items.size());
                        void* buf = malloc(message_size);

                        auto serialize = [](void* mem, WorkItem& item, TCPMetaInfo& info) -> size_t {
                            info.payload_size = item.ByteSizeLong();
                            info.package_type = TcpPackageType::WORK;
                            memcpy(mem, &info, sizeof(TCPMetaInfo));
                            item.SerializeToArray(static_cast<char*>(mem) + sizeof(TCPMetaInfo), item.ByteSizeLong());
                            return item.ByteSizeLong() + sizeof(TCPMetaInfo);
                        };

                        void* tmp_buf = buf;
                        TCPMetaInfo message_info;
                        for (auto& item : items) {
                            size_t offest = serialize(tmp_buf, item, message_info);
                            tmp_buf = static_cast<char*>(tmp_buf) + offest;
                        }

                        // Send item(s) to clients
                        std::cout << "Total message size: " << message_size << std::endl;
                        server.sendToAllOfType(UnitType::COMPUTE_UNIT, static_cast<const char*>(buf), message_size);

                        free(buf);
                    } break;
                    case 3: {
                        const size_t itemCount = 10;

                        std::vector<WorkItem> items;
                        size_t itemsByteSize = 0;
                        for (size_t i = 0; i < itemCount; ++i) {
                            auto item = tuddbs::Utility::generateRandomWorkItem(true);
                            item.set_itemid(i);
                            itemsByteSize += item.ByteSizeLong();
                            items.push_back(item);
                        }

                        std::cout << "Serializing " << itemCount << " items..." << std::endl;

                        const size_t message_size = itemsByteSize + (sizeof(TCPMetaInfo) * items.size());
                        void* buf = malloc(message_size);

                        void* tmp_buf = buf;
                        TCPMetaInfo message_info;
                        message_info.package_type = TcpPackageType::WORK;
                        for (auto& item : items) {
                            size_t offest = tuddbs::Utility::serializeItemToMemory<WorkItem>(tmp_buf, item, message_info);
                            tmp_buf = static_cast<char*>(tmp_buf) + offest;
                        }

                        // Send overflow/incomplete chunks
                        std::cout << "Total message size: " << message_size << std::endl;

                        const size_t end_first_package = message_size / 3;
                        std::cout << "Sending first message part: " << end_first_package << std::endl;
                        server.sendToAllOfType(UnitType::COMPUTE_UNIT, static_cast<const char*>(buf), end_first_package);

                        std::cout << "Waiting 2s..." << std::endl;
                        {
                            using namespace std::chrono_literals;
                            std::this_thread::sleep_for(2s);
                        }

                        size_t size_left = message_size - end_first_package;

                        const size_t end_second_package = size_left / 2;
                        std::cout << "Sending second message part: " << end_second_package << std::endl;
                        server.sendToAllOfType(UnitType::COMPUTE_UNIT, static_cast<const char*>(buf) + end_first_package, end_second_package);

                        std::cout << "Waiting 2s..." << std::endl;
                        {
                            using namespace std::chrono_literals;
                            std::this_thread::sleep_for(2s);
                        }

                        size_left -= end_second_package;
                        std::cout << "Sending last message part: " << size_left << std::endl;
                        server.sendToAllOfType(UnitType::COMPUTE_UNIT, static_cast<const char*>(buf) + end_first_package + end_second_package, size_left);

                        free(buf);
                    } break;
                    case 4: {
                        server.clear_aborted();
                    } break;
                }
            }
        }
    }
}
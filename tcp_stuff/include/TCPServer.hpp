#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <string>

#include "TCPCommon.hpp"

namespace tuddbs {

struct ClientInfo {
    ClientHandle handle = -1;
    std::thread* receiver = nullptr;
    size_t unprocessed_bytes = 0;
    UnitType type = UnitType::UNDEFINED_UNIT_TYPE;
    uint64_t uuid = 0;
    std::string prettyName;
    std::atomic<bool> abort{false};

    ClientInfo() = default;
    ~ClientInfo();

    // Non-copyable (contains atomic and thread pointer)
    ClientInfo(const ClientInfo&) = delete;
    ClientInfo& operator=(const ClientInfo&) = delete;

    void release();

    bool operator==(const ClientInfo& other) const {
        return this->handle == other.handle;
    }

   private:
    bool cleanupDone = false;
};

class TCPServer {
   public:
    typedef std::function<void(ClientHandle)> ConnectCallback;

   private:
    int _port;
    std::thread t;
    ConnectCallback connectCallback;
    std::function<void(int)> disconnectCallback = nullptr;

   public:
    TCPServer(int port);
    ~TCPServer();

    void setConnectCallback(ConnectCallback cc);

    void start();

    void closeConnection();

    bool hasClients() const;
    void clientDebugInfo() const;
    ClientInfo* getClientByUuid(uint64_t uuid) const;
    std::vector<std::pair<std::string, uint64_t>> getUuidForUnitType(UnitType type);

    bool sendTo(ClientInfo* info, const char* data, uint32_t len);
    void sendToAll(const char* data, uint32_t len);
    void sendToAllOfType(UnitType type, const char* data, uint32_t len);
    void sendToAnyOfType(UnitType type, const char* data, uint32_t len);
    void rerouteToAnyOfType(UnitType type, const uint64_t original_uuid, const char* original_data, uint32_t original_len);
    void addCallback(TcpPackageType type, ReceiveCallback cb);
    void setTimeoutToHandle(ClientHandle handle, const size_t sec, const size_t usec);

    void clear_aborted();
    static void freeHandle(ClientHandle handle);
    std::string monitorInfoToString();

    static int getCurrentThreadSocket();

    void setDisconnectCallback(std::function<void(int)> cb) {
     this->disconnectCallback = cb;
    }

   private:
    std::atomic<bool> globalAbort{false};
    bool cleanupDone = false;
    ServerHandle serverHandle = -1;

    CallbackMap callbacks;
    std::map<UnitType, std::vector<ClientInfo*>> clientMap;
    std::unordered_map<uint64_t, ClientInfo*> clientUuidMap;
    mutable std::recursive_mutex cl_mutex;
    void acceptLoop();
    void removeClient(ClientInfo* info);
    void removeClient(UnitType type, ClientInfo* info);
};

}  // namespace tuddbs
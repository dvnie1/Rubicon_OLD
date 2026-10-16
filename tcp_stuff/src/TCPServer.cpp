#include "TCPServer.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <Utility.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "UnitDefinition.pb.h"
#include "WorkResponse.pb.h"
#include "Logger.hpp"

namespace tuddbs {

/**
 * @brief Construct a new TCPServer::TCPServer object
 *
 * @param port
 */

thread_local int t_current_socket_fd = -1;
int TCPServer::getCurrentThreadSocket() {
    return t_current_socket_fd;
}


TCPServer::TCPServer(int port) : _port(port) {
    auto clientInfoReceiveCallback = [this](ClientInfo* client) -> void {
        auto waitReadable = [&](int fd, int timeout_ms) -> bool {
            pollfd pfd{};
            pfd.fd = fd;
            pfd.events = POLLIN;

            for (;;) {
                int r = ::poll(&pfd, 1, timeout_ms);
                if (r > 0) {
                    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) return false;
                    return (pfd.revents & POLLIN) != 0;
                }
                if (r == 0) return false;      // timeout
                if (errno == EINTR) continue;  // interrupted poll
                return false;                  // poll error
            }
        };

        const size_t MAX_DATA_SIZE = 1024 * 1024 * 64;
        void* msg_buffer = std::malloc(MAX_DATA_SIZE);

        if (!msg_buffer) {
            std::cerr << "[ClientInfoCallback] Memory allocation failed!\n";
            client->abort.store(true);
            return;
        }

        std::memset(msg_buffer, 0, MAX_DATA_SIZE);

        VLOG("[ClientInfoCallback] Initialized client with Buffer MaxSize: " << MAX_DATA_SIZE << " Byte.");

        // Use a finite timeout so we can periodically check client->abort.
        // -1 would block forever in poll().
        const int POLL_TIMEOUT_MS = 200;

        while (!client->abort.load()) {
            t_current_socket_fd = client->handle;
            const size_t available_space = MAX_DATA_SIZE - client->unprocessed_bytes;
            if (available_space == 0) {
                std::cerr << "[ClientInfoCallback] Buffer overflow risk detected (" << client->unprocessed_bytes << " unprocessed bytes)!\n";
                client->abort.store(true);
                break;
            }

            // Wait until socket is readable (or error/hup).
            if (!waitReadable(client->handle, POLL_TIMEOUT_MS)) {
                // If we timed out, just loop again so we can re-check client->abort.
                // But if the socket actually died, poll() also returns false (ERR/HUP/NVAL).
                // Distinguish timeout vs error by checking poll again with 0 or track it:
                // simplest: attempt a non-blocking recv? Instead, we keep it conservative:
                // If you want, you can log only on actual disconnect cases (recv==0 / real errno).
                continue;
            }

            ssize_t received_bytes = ::recv(client->handle, reinterpret_cast<char*>(msg_buffer) + client->unprocessed_bytes, available_space, 0);

            if (received_bytes > 0) {
                tuddbs::Utility::extractItemsModifyBuffer(
                    msg_buffer,
                    client->unprocessed_bytes + static_cast<size_t>(received_bytes),
                    msg_buffer,
                    client->unprocessed_bytes,
                    &callbacks,
                    true);
                continue;
            }

            if (received_bytes == 0) {
                VLOG("[ClientInfoCallback] Client disconnected (recv==0).");
                client->abort.store(true);
                break;
            }

            // received_bytes < 0
            int e = errno;
            if (e == EINTR) {
                continue;  // retry loop
            }
            if (e == EAGAIN || e == EWOULDBLOCK) {
                // Can happen even after poll() due to race conditions; just try again.
                continue;
            }

            std::cerr << "[ClientInfoCallback] recv() failed: " << e << " (" << std::strerror(e) << ")\n";
            client->abort.store(true);
            break;
        }

        std::free(msg_buffer);
    };

    TCPServer::ConnectCallback server_connect_callback = [this, clientInfoReceiveCallback](ClientHandle handle) -> void {
        TCPMetaInfo info;
        const size_t bufferSize = 1024 * 1024 * 64;
        void* buf = malloc(bufferSize);

        info.package_type = TcpPackageType::UPDATE_UNIT_TYPE;
        info.payload_size = 0;

        // Set TCP_NODELAY and TCP_QUICKACK to avoid stalling of the TCP stack with Nagle's algorithm, as we sent plenty of small messages.
        {
            int yes = 1;
            int result = setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&yes), sizeof(int));
            if (result != 0) {
                VLOG("[Warning] Could not set TCP_NODELAY");
            }
            result = setsockopt(handle, IPPROTO_TCP, TCP_QUICKACK, reinterpret_cast<char*>(&yes), sizeof(int));
            if (result != 0) {
                VLOG("[Warning] Could not set TCP_QUICKACK");
            }
        }

        // Request the UnitDefinition protobuf struct from the just-connected client.
        int res = 0;
        if ((res = send(handle, &info, info.bytesize(), kSendFlags)) <= 0) {
            VLOG("[TCPServer] ConnectCallback: Could not send UpdateUnitType to client with handle " << handle);
        }

        // If the client takes longer than 2s to responed, drop it.
        setTimeoutToHandle(handle, 2, 0);
        ssize_t received_bytes = recv(handle, reinterpret_cast<char*>(buf), bufferSize - 1, 0);

        if (received_bytes > 0) {
            // Check for correct message layout. TCP_START_DELIM should always appear first.
            if (*reinterpret_cast<uint32_t*>(buf) != TCP_START_DELIM) {
                VLOG("[UpdateUnitType] Error. I did not find a message delimiter at index 0. Discarding client.");
                freeHandle(handle);
            } else {
                // Message seems to be well-formed. Now parse it. Expected structure: [ TCP_START_DELIM | TCPMetaInfo | payload ]
                TCPMetaInfo update_info;
                std::memcpy(&update_info, buf, sizeof(TCPMetaInfo));

                if (static_cast<size_t>(received_bytes) < update_info.bytesize()) {
                    //VLOG("[UpdateUnitType] Error. Please implement continuous receive for segmented unit info packages.");
                    exit(-1);
                }
                // Only accept the UpdateUnitType response at this time. Drop any client which does not obey the protocol.
                if (update_info.package_type != TcpPackageType::UPDATE_UNIT_TYPE) {
                    VLOG("[UpdateUnitType] Received something else than a UnitUpdate info. Discarding client.");
                    freeHandle(handle);
                }
                // Let's parse the payload and see if we received a correct UnitDefinition protobuf message.
                try {
                    UnitDefinition unit;
                    unit.ParseFromArray(reinterpret_cast<char*>(buf) + sizeof(TCPMetaInfo), update_info.payload_size);
                    VLOG("[UpdateUnitType] Unit Info:" << unit.DebugString());
                    ClientInfo* cl_info = new ClientInfo();
                    cl_info->handle = handle;
                    cl_info->prettyName = unit.prettyname();
                    // enum class values are encoded as their respective integer value on the wire.
                    const UnitType parsed_type = static_cast<UnitType>(unit.unit_type());
                    cl_info->type = parsed_type;
                    bool uuid_collision_resolved = false;
                    setTimeoutToHandle(handle, 1, 0);
                    // Probe if another client already uses the same UUID. It should actually never happen with random 64bit integers, but here we are for good measure.
                    do {
                        cl_mutex.lock();
                        const bool collision = clientUuidMap.contains(update_info.src_uuid);
                        cl_mutex.unlock();
                        if (collision) {
                            VLOG("[TCPServer] Found a colliding UUID. Requesting new UUID from client. Was: " << update_info.src_uuid);
                            // Collision detected, trigger UUID regeneration.
                            info.package_type = TcpPackageType::UUID_COLLISION;
                            info.payload_size = 0;
                            if ((res = send(handle, &info, info.bytesize(), kSendFlags)) <= 0) {
                                VLOG("[TCPServer] ConnectCallback: Could not send UpdateUnitType to client with handle " << handle);
                                freeHandle(handle);
                                delete cl_info;
                                return;
                            }
                            received_bytes = recv(handle, reinterpret_cast<char*>(buf), bufferSize - 1, 0);
                            // If receiving failed or the message is ill-formed, abort the setup process.
                            if (received_bytes <= 0 || (*reinterpret_cast<uint32_t*>(buf) != TCP_START_DELIM)) {
                                VLOG("[UpdateUnitType] Error during UUID collision handling. Terminating client connection.");
                                freeHandle(handle);
                                delete cl_info;
                                return;
                            }
                            // Receiving another answer than an updated UUID cancels the setup process.
                            if (update_info.package_type != TcpPackageType::UUID_COLLISION) {
                                VLOG("[UpdateUnitType] Error during UUID collision handling. Received wrong response: " << TcpPackageType_Name(update_info.package_type) << " -- Dropping client.");
                                freeHandle(handle);
                                delete cl_info;
                                return;
                            }
                        } else {
                            // Collision is resolved, update client info struct and add it to the server
                            uuid_collision_resolved = true;
                            cl_info->uuid = update_info.src_uuid;
                            std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
                            clientMap[parsed_type].push_back(cl_info);
                            clientUuidMap[update_info.src_uuid] = cl_info;
                        }
                    } while (!uuid_collision_resolved);
                    // We are now ready to patiently wait for any data from this connection and spawn a receiver thread for this client.
                    setTimeoutToHandle(handle, 0, 0);
                    cl_info->receiver = new std::thread(clientInfoReceiveCallback, cl_info);
                    VLOG("[UpdateUnitType] Added a new " << UnitType_Name(parsed_type) << " with uuid=<" << cl_info->uuid << ">");
                } catch (const std::exception& e) {
                    VLOG(e.what());
                }
            }
        } else {
            std::cerr << "[ConnectCallback] Error. Did not receive a response. Discarding client." << std::endl;
            freeHandle(handle);
        }

        free(buf);
    };
    setConnectCallback(server_connect_callback);
}

/**
 * @brief Destroy the TCPServer::TCPServer object
 *
 */
TCPServer::~TCPServer() {
    closeConnection();
}

/**
 * @brief Set the callback function, which is invoked when a new TCP connection is established.
 *
 * @param cc The connect callback function.
 */
void TCPServer::setConnectCallback(ConnectCallback cc) {
    this->connectCallback = cc;
}

/**
 * @brief Check if any clients are connected to the server.
 *
 * @return true At least one client is connected, its unit type is irrelevant.
 * @return false No client is connected.
 */
bool TCPServer::hasClients() const {
    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    size_t clientCount = 0;
    for (auto it = clientMap.begin(); it != clientMap.end(); ++it) {
        clientCount += it->second.size();
    }
    return clientCount > 0;
}

/**
 * @brief Fetch a client info struct based on a given UUID.
 *
 * @param uuid The 64bit identifier which is provided during the connection setup process.
 * @return ClientInfo* Information container which represents a connected client.
 */
ClientInfo* TCPServer::getClientByUuid(uint64_t uuid) const {
    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    if (clientUuidMap.contains(uuid)) {
        return clientUuidMap.at(uuid);
    }
    return nullptr;
}

/**
 * @brief Fetch all pairs of clients pretty name and their respective UUID.
 *
 * @param type Determines the unit type for which the pairs are returned.
 * @return std::vector<std::pair<std::string, uint64_t>> Vector containing name and uuid pairs for all clients of the requested type.
 */
std::vector<std::pair<std::string, uint64_t>> TCPServer::getUuidForUnitType(UnitType type) {
    if (!hasClients()) {
        return {};
    }
    clear_aborted();
    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    if (type == UnitType::UNDEFINED_UNIT_TYPE) {
        std::vector<std::pair<std::string, uint64_t>> uuids;
        for (auto it = clientMap.begin(); it != clientMap.end(); ++it) {
            auto cl_info_vec = it->second;
            auto cl_it = cl_info_vec.cbegin();
            while (cl_it != cl_info_vec.cend()) {
                uuids.push_back({(*cl_it)->prettyName, (*cl_it)->uuid});
                ++cl_it;
            }
        }
        return uuids;
    } else if (clientMap.contains(type)) {
        auto cl_info_vec = clientMap.at(type);
        auto cl_it = cl_info_vec.cbegin();
        std::vector<std::pair<std::string, uint64_t>> uuids;
        uuids.reserve(cl_info_vec.size());
        while (cl_it != cl_info_vec.cend()) {
            uuids.push_back({(*cl_it)->prettyName, (*cl_it)->uuid});
            ++cl_it;
        }
        return uuids;
    }
    return {};
}

/**
 * @brief Send data to all connected clients.
 *
 * @param data Binary encoded message.
 * @param len data size in bytes.
 */
void TCPServer::sendToAll(const char* data, uint32_t len) {
    if (!hasClients()) {
        return;
    }
    VLOG("Sending text to all clients.");

    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    auto it = clientMap.begin();
    while (it != clientMap.end()) {
        // The map iterator holds a pair of UnitType and client info vectors.
        auto& cl_info_vec = it->second;
        auto cl_it = cl_info_vec.begin();
        while (cl_it != cl_info_vec.end()) {
            ClientHandle sck = (*cl_it)->handle;
            int res = send(sck, data, len, kSendFlags);
            if (res < 0) {
                VLOG("Res is < 0, erasing client.");
                removeClient(it->first, *cl_it);
                continue;
            } else {
                VLOG("Sent " << res << " chars to remote.");
            }
            ++cl_it;
        }
        ++it;
    }
}

/**
 * @brief Send data to all connected clients of a specific type.
 *
 * @param type The unit type which qualifies a client to receive this message.
 * @param data Binary encoded message.
 * @param len data size in bytes.
 */
void TCPServer::sendToAllOfType(UnitType type, const char* data, uint32_t len) {
    if (!hasClients()) {
        return;
    }

    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    if (clientMap.contains(type)) {
        VLOG("[TCPServer::sendToAllOfType] Sending data to all clients of type: " << UnitType_Name(type));

        auto cl_info_vec = clientMap[type];
        auto cl_it = cl_info_vec.begin();
        while (cl_it != cl_info_vec.end()) {
            ClientHandle sck = (*cl_it)->handle;
            int res = send(sck, data, len, kSendFlags);
            if (res < 0) {
                VLOG("[TCPServer::sendToAllOfType] Res is < 0, erasing client.");
                removeClient(type, *cl_it);
                continue;
            } else {
                VLOG("[TCPServer::sendToAllOfType] Sent " << res << " chars to remote.");
            }
            ++cl_it;
        }
    } else {
        VLOG("[TCPServer::sendToAllOfType] No clients of type: " << UnitType_Name(type));
    }
}

/**
 * @brief Print debug information about currently connected clients.
 *
 */
void TCPServer::clientDebugInfo() const {
    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    VLOG("Unique UnitTypes: " << clientMap.size() << "\nClients per type:");
    for (const auto& it : clientMap) {
        VLOG(UnitType_Name(it.first) << ": " << it.second.size());
    }
}

/**
 * @brief Send data to a random client of a specific type.
 *
 * @param type The unit type which qualifies a client to receive this message.
 * @param data Binary encoded message.
 * @param len data size in bytes.
 */
void TCPServer::sendToAnyOfType(UnitType type, const char* data, uint32_t len) {
    if (!hasClients()) {
        VLOG("[TCPServer::sendToAnyOfType] No Clients are currently connected.");
        return;
    }
    VLOG("[TCPServer::sendToAnyOfType] Sending WorkItem to a random Client.");
    clientDebugInfo();

    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    if (clientMap.contains(type)) {
        bool success = false;
        while (!success) {
            auto& cl_info_vec = clientMap[type];
            if (cl_info_vec.empty()) {
                VLOG("[TCPServer::sendToAnyOfType] No Clients for type " << static_cast<size_t>(type) << " are currently connected.");
                break;
            }
            const size_t idx = Utility::generateRandomNumber(0, cl_info_vec.size() - 1);
            auto client = cl_info_vec[idx];
            VLOG("[TCPServer::sendToAnyOfType] Client count: " << cl_info_vec.size() << " - I use: " << idx << " (Abort: " << std::boolalpha << client->abort.load() << ")");
            if (sendTo(client, data, len)) {
                success = true;
            } else {
                VLOG("[TCPServer::sendToAnyOfType] Original client did not answer. Retrying with new client.");
            }
        }
    }
}

/**
 * @brief Reroute data to a random client of a specific type. Exclude the original recipient.
 *
 * @param type The unit type which qualifies a client to receive this message.
 * @param original_uuid The original recipient, which cannot serve the request.
 * @param data Binary encoded message.
 * @param len data size in bytes.
 */
void TCPServer::rerouteToAnyOfType(UnitType type, const uint64_t original_uuid, const char* original_data, uint32_t original_len) {
    if (!hasClients()) {
        VLOG("[TCPServer::rerouteToAnyOfType] No Clients are currently connected.");
        return;
    }
    VLOG("[TCPServer::rerouteToAnyOfType] Rerouting WorkItem to a random Client, ignoring " << original_uuid);
    clientDebugInfo();

    auto find_and_send = [this, &original_uuid](const auto& clients, const size_t start, const size_t end, const char* data, const uint32_t len) -> bool {
        for (size_t i = start; i != end; ++i) {
            auto potential_target = clients[i];
            if (potential_target->uuid != original_uuid) {
                if (sendTo(potential_target, data, len)) {
                    return true;
                } else {
                    VLOG("[TCPServer::rerouteToAnyOfType] Rerouting client did not answer. Retrying with new client.");
                }
            }
        }
        return false;
    };

    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    if (clientMap.contains(type)) {
        bool success = false;
        auto& cl_info_vec = clientMap[type];
        if (cl_info_vec.empty()) {
            VLOG("[TCPServer::rerouteToAnyOfType] No Clients for type " << static_cast<size_t>(type) << " are currently connected.");
            return;
        }
        const size_t idx = Utility::generateRandomNumber(0, cl_info_vec.size() - 1);
        success = find_and_send(cl_info_vec, idx, cl_info_vec.size(), original_data, original_len);
        if (!success) {
            success = find_and_send(cl_info_vec, 0, idx, original_data, original_len);
        }
    }
}

// helper: wait until socket is writable (timeout_ms: -1 = infinite)
static bool waitWritable(int fd, int timeout_ms) {
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLOUT;

    for (;;) {
        int r = ::poll(&pfd, 1, timeout_ms);
        if (r > 0) {
            // If poll signals error/hup/nval, treat as not writable / dead.
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) return false;
            return true;
        }
        if (r == 0) return false;      // timeout
        if (errno == EINTR) continue;  // interrupted poll, retry
        return false;                  // real poll error
    }
}

/**
 * @brief Send data to a specific client.
 *
 * @param cl_info The client info structure of the target client.
 * @param data Binary encoded message.
 * @param len data size in bytes.
 * @return true Sending the message succeeded.
 * @return false Sending the message failed, either the client has already disconnected or the sending process failed.
 */
bool TCPServer::sendTo(ClientInfo* cl_info, const char* data, uint32_t len) {
    if (cl_info->abort.load()) {
        removeClient(cl_info);
        return false;
    }

    const char* p = data;
    size_t remaining = len;

    while (remaining > 0) {
        // On Linux, prevent SIGPIPE if peer already closed.
        int flags = kSendFlags;
#ifdef MSG_NOSIGNAL
        flags |= MSG_NOSIGNAL;
#endif

        ssize_t res = ::send(cl_info->handle, p, remaining, flags);

        if (res > 0) {
            p += res;
            remaining -= static_cast<size_t>(res);
            continue;
        }

        if (res == 0) {
            // Rare for send(); treat as failure to make progress.
            std::cerr << "[TCPServer::sendTo] send() returned 0, erasing client.\n";
            removeClient(cl_info);
            return false;
        }

        // res < 0
        int e = errno;

        if (e == EINTR) {
            continue;  // retry immediately
        } else if (e == EAGAIN || e == EWOULDBLOCK) {
            // Socket not writable right now. Wait until it is, then retry.
            // Choose a timeout that makes sense for your app (e.g., 5000 ms).
            if (!waitWritable(cl_info->handle, /*timeout_ms=*/5000)) {
                std::cerr << "[TCPServer::sendTo] send() would block and timed out/failed, erasing client.\n";
                removeClient(cl_info);
                return false;
            }
            continue;
        } else {
            std::cerr << "[TCPServer::sendTo] Error in TCPServer with errno " << e << " (" << std::strerror(e) << "), erasing client." << std::endl;
            removeClient(cl_info);
            return false;
        }
    }

    VLOG("[TCPServer::sendTo] Sent " << len << " chars to a " << UnitType_Name(cl_info->type) << " (\"" << cl_info->prettyName << "\")");
    return true;
}

/**
 * @brief Start the loop for accepting connections.
 *
 */
void TCPServer::start() {
    t = std::thread(&TCPServer::acceptLoop, this);
}

/**
 * @brief Remove all disconnected clients.
 *
 */
void TCPServer::clear_aborted() {
    VLOG("[TCPServer::clear_aborted] Removing all aborted clients.");
    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    size_t aborted_cnt = 0;
    for (auto it = clientMap.begin(); it != clientMap.end(); ++it) {
        auto& cl_vec = it->second;
        for (auto info_it = cl_vec.begin(); info_it != cl_vec.end();) {
            if ((*info_it)->abort.load()) {
                removeClient(it->first, *info_it);
                ++aborted_cnt;
                continue;
            }
            ++info_it;
        }
    }
    VLOG("[TCPServer::clear_aborted] I cleaned " << aborted_cnt << " clients.");
    clientDebugInfo();
}

/**
 * @brief Close and terminate the socket of a client.
 *
 * @param handle The file descriptor handle of the to-be-closed socket.
 */
void TCPServer::freeHandle(ClientHandle handle) {
    ::shutdown(handle, SHUT_RDWR);
    close(handle);
}

/**
 * @brief Create an informative string holding all currently connected clients with their pretty names and IPs.
 *
 * @return std::string A string representation of the server state.
 */
std::string TCPServer::monitorInfoToString() {
    clear_aborted();
    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    std::stringstream ss;
    for (auto it = clientMap.begin(); it != clientMap.end(); ++it) {
        ss << UnitType_Name(it->first) << " [" << std::endl;
        const auto& cl_vec = it->second;
        for (auto info_it = cl_vec.begin(); info_it != cl_vec.end(); ++info_it) {
            struct sockaddr_in peername;
            socklen_t addr_len = sizeof(sockaddr_in);
            getpeername((*info_it)->handle, reinterpret_cast<sockaddr*>(&peername), &addr_len);
            ss << "  " << (*info_it)->prettyName << " (" << inet_ntoa(peername.sin_addr) << ")" << std::endl;
        }
        ss << "]" << std::endl;
    }
    return ss.str();
}

/**
 * @brief Terminate the server.
 *
 */
void TCPServer::closeConnection() {
    if (!cleanupDone) {
        globalAbort.store(true);
        {
            std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
            for (auto it = clientMap.begin(); it != clientMap.end(); ++it) {
                auto& cl_vec = it->second;
                for (auto info_it = cl_vec.begin(); info_it != cl_vec.end(); ++info_it) {
                    (*info_it)->release();
                    delete (*info_it);
                }
                cl_vec.clear();
            }
        }
        freeHandle(serverHandle);
        VLOG("[TCPServer::closeConnection] Waiting for join...");
        if (t.joinable()) {
            t.join();
        }
        VLOG("[TCPServer::closeConnection] Done!");
        cleanupDone = true;
    }
}

/**
 * @brief Register a callback function, which is called whenever a message with a specific type arrives.
 *
 * @param type The TCP message type, found in a TCPMetaInfo struct, which triggers a callback.
 * @param cb The callback function for a given TcpPackageType.
 */
void TCPServer::addCallback(TcpPackageType type, ReceiveCallback cb) {
    callbacks[type] = cb;
}

/**
 * @brief Convenience function to set a timeout on a socket handle. sec and used are added for the total value.
 *
 * @param handle The socket handle whose timeout value is to be updated.
 * @param sec Timeout component in seconds.
 * @param usec Timeout component in microseconds.
 */
void TCPServer::setTimeoutToHandle(ClientHandle handle, const size_t sec, const size_t usec) {
    struct timeval timeout;
    timeout.tv_sec = sec;
    timeout.tv_usec = usec;
    setsockopt(handle, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
}
/**
 * @brief The event loop of the server. Accepts new connetions as long as no global abort is requested.
 *
 */
void TCPServer::acceptLoop() {
    serverHandle = socket(AF_INET, SOCK_STREAM, 0);
    if (serverHandle == 0) {
        throw std::runtime_error("[TCPServer::acceptLoop] Error. Failed to create server socket.");
    }

    int opt = 1;
    setsockopt(serverHandle, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(_port);

    if (bind(serverHandle, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        throw std::runtime_error("[TCPServer::acceptLoop] Error. Failed to bind to socket.");
    }
    if (listen(serverHandle, 3) < 0) {
        throw std::runtime_error("[TCPServer::acceptLoop] Error. Failed to listen from socket.");
    }

    while (!globalAbort.load()) {
        int addrlen = sizeof(address);
        ClientHandle sck = accept(serverHandle, reinterpret_cast<sockaddr*>(&address), reinterpret_cast<socklen_t*>(&addrlen));
        if (globalAbort.load()) {
            break;
        }
        if (sck < 0) {
            throw std::runtime_error("[TCPServer::acceptLoop] Failed to accept!");
        }
        VLOG("[TCPServer::acceptLoop] Received a connection from " << inet_ntoa(address.sin_addr) << " on port " << address.sin_port);

        // inform the connect callback
        if (connectCallback) {
            connectCallback(sck);
        }
    }
}

/**
 * @brief Remove a specific client from the server, e.g. if it disconnected or does not answer anymore.
 *
 * @param info The to-be-removed client info.
 */
void TCPServer::removeClient(ClientInfo* info) {
    VLOG("[TCPServer::removeClient] Removing Client by Info-Struct");
    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    for (auto it = clientMap.begin(); it != clientMap.end(); ++it) {
        auto& cl_vec = it->second;
        for (auto info_it = cl_vec.begin(); info_it != cl_vec.end(); ++info_it) {
            if ((*info_it)->handle == info->handle) {
                removeClient(it->first, *info_it);
                return;
            }
        }
    }
}

/**
 * @brief Remove a specific client from the server, e.g. if it disconnected or does not answer anymore.
 *
 * @param type The unit type of the client.
 * @param info The to-be-removed client info.
 */
void TCPServer::removeClient(UnitType type, ClientInfo* info) {
    VLOG("[TCPServer::removeClient] Removing Client by Type and Info-Struct");
    std::lock_guard<std::recursive_mutex> _lk(cl_mutex);
    auto& cl_vec = clientMap[type];
    auto cl_it = std::find(cl_vec.begin(), cl_vec.end(), info);
    if (cl_it != cl_vec.end()) {
        VLOG("[TCPServer::removeClient] Removing uuid<" << info->uuid << "> from aliases.");
        clientUuidMap.erase(info->uuid);
        info->release();
        delete info;
        cl_vec.erase(cl_it);
    }
}

/**
 * @brief Tear down an initialized client info structure.
 *
 */
void ClientInfo::release() {
    VLOG("[ClientInfo::release] Releasing ClientInfo for handle " << handle << "...");
    abort.store(true);
    TCPServer::freeHandle(handle);
    if (receiver) {
        VLOG("[ClientInfo::release] Joining...");
        receiver->join();
        VLOG("[ClientInfo::release] Done!");
        delete receiver;
        receiver = nullptr;
    }
    cleanupDone = true;
}

/**
 * @brief Destroy the Client Info:: Client Info object
 *
 */
ClientInfo::~ClientInfo() {
    if (!cleanupDone) {
        release();
    }
}

}  // namespace tuddbs
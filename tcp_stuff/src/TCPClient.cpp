#include "TCPClient.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <poll.h>

#include <functional>
#include <iostream>

#include "Utility.hpp"

namespace tuddbs {

TCPClient::TCPClient(std::string ip, int port, bool verbose) : _server_ip(ip), _session_uuid(0), _port(port), _verbose(verbose) {
    auto update_uuid_on_collision = [this](tuddbs::TCPMetaInfo* meta, void* data, size_t len) -> void {
        std::cout << "[TCPClient] UUID collision signaled from TCPServer. Creating a new UUID." << std::endl;
        generateSessionUuid();
        TCPMetaInfo info;
        info.package_type = TcpPackageType::UUID_COLLISION;
        info.src_uuid = getUuid();
        notifyHost(&info, sizeof(TCPMetaInfo));
    };
    addCallback(TcpPackageType::UUID_COLLISION, update_uuid_on_collision);
}

TCPClient::~TCPClient() {
    closeConnection();
}

void TCPClient::generateSessionUuid() {
    std::mt19937_64 gen(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<long long int> dist(0, std::llround(std::pow(2, 64) - 1));
    _session_uuid = dist(gen);
}

void TCPClient::start() {
    serverHandle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (serverHandle < 0) {
        throw std::runtime_error("Could not open socket.");
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_port = htons(_port);

    if (inet_pton(AF_INET, _server_ip.c_str(), &address.sin_addr) <= 0) {
        throw std::runtime_error("Could not set target IP.");
    }

    if (connect(serverHandle, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        throw std::runtime_error("Could not connect to server.");
    }

    int yes = 1;
    int result = setsockopt(serverHandle, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&yes), sizeof(int));
    if (result != 0) {
        std::cout << "[Warning] Could not set TCP_NODELAY" << std::endl;
    }
    result = setsockopt(serverHandle, IPPROTO_TCP, TCP_QUICKACK, reinterpret_cast<char*>(&yes), sizeof(int));
    if (result != 0) {
        std::cout << "[Warning] Could not set TCP_QUICKACK" << std::endl;
    }
    generateSessionUuid();
    t = std::thread(&TCPClient::listenLoop, this);
}

void TCPClient::closeConnection() {
    globalAbort.store(true);
    ::shutdown(serverHandle, SHUT_RDWR);
    close(serverHandle);
    if (t.joinable()) {
        t.join();
    }
}

bool TCPClient::isConnected() const {
    return !globalAbort.load();
}

void TCPClient::notifyHost(void* data, size_t len) {
    ssize_t res = send(serverHandle, data, len, kSendFlags);
    (void)res;  // TODO: handle partial sends
}

void TCPClient::textResponse(const std::string text, uint64_t tgt_uuid) {
    TCPMetaInfo info;
    info.package_type = TcpPackageType::TEXT;
    info.payload_size = text.size();
    info.src_uuid = getUuid();
    info.tgt_uuid = tgt_uuid;
    size_t msgSz = sizeof(TCPMetaInfo) + text.size();
    void* buf = malloc(msgSz);
    if (!buf) {
        std::cerr << "[TCPClient::textResponse] Wasn't able to allocate the requested memory!" << std::endl;
        exit(-1);
    }
    memcpy(buf, &info, sizeof(TCPMetaInfo));
    memcpy(reinterpret_cast<char*>(buf) + sizeof(TCPMetaInfo), text.c_str(), text.size());
    notifyHost(buf, msgSz);
    free(buf);
}

uint64_t TCPClient::getUuid() const {
    return _session_uuid;
}

void TCPClient::addCallback(TcpPackageType type, ReceiveCallback cb) {
    callbacks[type] = cb;
}

static bool waitReadable(int fd, int timeout_ms) {
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;

    for (;;) {
        int r = ::poll(&pfd, 1, timeout_ms);
        if (r > 0) {
            // readable or error/hup
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) return false;
            if (pfd.revents & POLLIN) return true;
            // Other events are unusual; treat as failure to be safe
            return false;
        }
        if (r == 0) return false;          // timeout
        if (errno == EINTR) continue;      // interrupted poll, retry
        return false;                      // poll error
    }
}

void TCPClient::listenLoop() {
    const size_t MAX_DATA_SIZE = 1024 * 1024 * 64;
    void* msg_buffer = std::malloc(MAX_DATA_SIZE);
    if (!msg_buffer) {
        std::cerr << "[TCPClient::listenLoop] Wasn't able to allocate the requested memory!" << std::endl;
        exit(-1);
    }
    std::memset(msg_buffer, 0, MAX_DATA_SIZE);

    unprocessed_bytes = 0;

    std::cout << "[TCPClient::listenLoop] Initialized client with Buffer MaxSize: " << MAX_DATA_SIZE << " Byte.\n";

    // Choose a poll timeout:
    // -1 = wait forever, 0 = do not wait, >0 = ms
    const int POLL_TIMEOUT_MS = -1;

    while (!globalAbort.load()) {
        // If you want responsiveness to globalAbort without busy-waiting,
        // use a finite timeout (e.g. 200ms) and check globalAbort each loop.
        if (!waitReadable(serverHandle, POLL_TIMEOUT_MS)) {
            if (!globalAbort.load()) {
                std::cerr << "[TCPClient::listenLoop] poll indicates error/hup/nval or timed out. Terminating.\n";
                const std::lock_guard<std::mutex> lk(channel_mutex);
                globalAbort.store(true);
                channel_cv.notify_all();
            }
            break;
        }

        void* receive_buffer = reinterpret_cast<char*>(msg_buffer) + unprocessed_bytes;
        const size_t available_buffer_size = MAX_DATA_SIZE - 1 - unprocessed_bytes;

        if (available_buffer_size == 0) {
            std::cerr << "[TCPClient::listenLoop] Buffer full (" << unprocessed_bytes << " unprocessed bytes). Message too large / parser not consuming. Terminating.\n";
            const std::lock_guard<std::mutex> lk(channel_mutex);
            globalAbort.store(true);
            channel_cv.notify_all();
            break;
        }

        ssize_t received_bytes = ::recv(serverHandle, receive_buffer, available_buffer_size, 0);

        if (received_bytes > 0) {
            if (_verbose) std::cout << "Received " << received_bytes << " Bytes.\n";

            tuddbs::Utility::extractItemsModifyBuffer(msg_buffer, unprocessed_bytes + static_cast<size_t>(received_bytes), msg_buffer, unprocessed_bytes, &callbacks);
            continue;
        }

        if (received_bytes == 0) {
            // This is the real “connection closed” case.
            std::cout << "[TCPClient::listenLoop] Peer closed connection (recv==0). Terminating Receiver.\n";
        } else { // received_bytes < 0
            int e = errno;

            if (e == EINTR) {
                continue; // retry loop (poll + recv again)
            }
            if (e == EAGAIN || e == EWOULDBLOCK) {
                // Can happen with non-blocking sockets even after poll (race), just loop back.
                continue;
            }

            std::cerr << "[TCPClient::listenLoop] recv error: " << e << " (" << std::strerror(e) << "). Terminating Receiver.\n";
        }

        // shutdown path
        if (!globalAbort.load()) {
            const std::lock_guard<std::mutex> lk(channel_mutex);
            globalAbort.store(true);
            channel_cv.notify_all();
        }
        break;
    }

    std::free(msg_buffer);
}

void TCPClient::waitUntilChannelClosed() {
    std::unique_lock<std::mutex> lk(channel_mutex);
    channel_cv.wait(lk, [this] { return globalAbort.load(); });
}

}  // namespace tuddbs
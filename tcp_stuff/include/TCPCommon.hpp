#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>

#include <sys/socket.h>  // MSG_NOSIGNAL, MSG_DONTWAIT

#include "UnitDefinition.pb.h"

namespace tuddbs {

static const uint32_t TCP_START_DELIM = 0x5ADB0BB1;

using ClientHandle = int;
using ServerHandle = int;

// Ensure no padding so the struct has a stable wire format across
// platforms and compilers.
#pragma pack(push, 1)
struct TCPMetaInfo {
    uint32_t message_delimiter = TCP_START_DELIM;
    UnitType unit_type = UnitType::UNDEFINED_UNIT_TYPE;
    uint32_t payload_size = 0;
    TcpPackageType package_type = TcpPackageType::UNDEFINED_PACKAGE_TYPE;
    uint64_t src_uuid = 0;
    uint64_t tgt_uuid = 0;

    uint64_t host_unit_uuid = 0;

    size_t bytesize() const { return sizeof(TCPMetaInfo) + payload_size; }
};
#pragma pack(pop)

// Common send flags used by both client and server.
// Defined inline to avoid ODR issues across translation units.
inline constexpr int kSendFlags = MSG_NOSIGNAL | MSG_DONTWAIT;

typedef std::function<void(TCPMetaInfo* meta, void* data, size_t len)> ReceiveCallback;
typedef std::map<TcpPackageType, ReceiveCallback> CallbackMap;

}  // namespace tuddbs
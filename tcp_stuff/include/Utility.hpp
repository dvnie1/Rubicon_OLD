#pragma once

#include <random>
#include <type_traits>

#include "TCPServer.hpp"
#include "WorkItem.pb.h"

namespace tuddbs {

class Utility {
   public:
    static size_t generateRandomNumber(size_t start, size_t end);

    static std::string generateRandomString();

    static WorkItem generateRandomWorkItem(bool verbose);

    template <typename T>
    static void generateRandomColumn(T* target, const size_t elementCount, const size_t seed = 0, const size_t lowerBound = 0, const size_t upperBound = 100) {
        std::mt19937 gen(seed == 0 ? std::chrono::high_resolution_clock::now().time_since_epoch().count() : seed);
        if (std::is_integral<T>::value) {
            std::uniform_int_distribution<> dis(lowerBound, upperBound);
            for (size_t i = 0; i < elementCount; i++) {
                target[i] = dis(gen);
            }
        } else if (std::is_floating_point<T>::value) {
            std::uniform_real_distribution<> dis(lowerBound, upperBound);
            for (size_t i = 0; i < elementCount; i++) {
                target[i] = dis(gen);
            }
        } else {
            throw std::runtime_error("Unsupported type for random generation.");
        }
    }

    template <typename Enumeration>
    static auto enum_as_integer(Enumeration const value)
        -> typename std::underlying_type<Enumeration>::type {
        return static_cast<typename std::underlying_type<Enumeration>::type>(value);
    }

    template <typename SerializeItemType>
    static size_t serializeItemToMemory(void* mem, SerializeItemType& item, TCPMetaInfo& info) {
        info.payload_size = item.ByteSizeLong();
        memcpy(mem, &info, sizeof(TCPMetaInfo));
        item.SerializeToArray(static_cast<char*>(mem) + sizeof(TCPMetaInfo), item.ByteSizeLong());
        return item.ByteSizeLong() + sizeof(TCPMetaInfo);
    }

    /**
     * This function extracts messages from the buffer and calls any provided callback per TcpPackageType.
     * The TcpPackageType is determined per message, e.g. per found TCPMetaInfo struct.
     * If the stream contains an incomplete message, the remaining bytes are moved to
     * out_message_buffer_start and the offset is written to out_unprocessed_bytes.
     * in_memory and out_message_buffer_start may be identical pointers.
     */
    static void extractItemsModifyBuffer(void* in_memory, ssize_t in_received_bytes, void* out_message_buffer_start, size_t& out_unprocessed_bytes, CallbackMap* callbacks = nullptr, bool verbose = false);
};
}  // namespace tuddbs
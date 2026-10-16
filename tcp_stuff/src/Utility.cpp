#include "Utility.hpp"

#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "Logger.hpp"
#include "../include/TCPServer.hpp"

namespace tuddbs {

size_t Utility::generateRandomNumber(size_t start, size_t end) {
    std::mt19937 mt(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<> dist(start, end);
    return dist(mt);
}

std::string Utility::generateRandomString() {
    const char letters[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::mt19937 mt(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<> char_dist(0, sizeof(letters) - 2);
    std::uniform_int_distribution<> len_dist(3, 20);
    std::string tmp;
    const size_t charCnt = len_dist(mt);
    tmp.reserve(charCnt);
    for (size_t count = 0; count < charCnt; ++count) {
        tmp += letters[char_dist(mt)];
    }
    return tmp;
}

WorkItem Utility::generateRandomWorkItem(bool verbose) {
    WorkItem item;
    item.set_planid(tuddbs::Utility::generateRandomNumber(0, 10));
    item.set_itemid(tuddbs::Utility::generateRandomNumber(0, 10));
    item.set_operatorid(static_cast<OperatorType>(tuddbs::Utility::generateRandomNumber(1, 5)));

    if (tuddbs::Utility::generateRandomNumber(0, 1)) {
        FilterItem* filter = item.mutable_filterdata();
        ColumnMessage* inputColumn = filter->mutable_inputcolumn();
        ColumnMessage* outputColumn = filter->mutable_outputcolumn();

        inputColumn->set_tabname(tuddbs::Utility::generateRandomString());
        inputColumn->set_colname(tuddbs::Utility::generateRandomString());
        inputColumn->set_coltype(static_cast<ColumnType>(tuddbs::Utility::generateRandomNumber(0, 2)));

        outputColumn->set_tabname(tuddbs::Utility::generateRandomString());
        outputColumn->set_colname(tuddbs::Utility::generateRandomString());
        outputColumn->set_coltype(static_cast<ColumnType>(tuddbs::Utility::generateRandomNumber(3, 4)));

        filter->set_filtertype(static_cast<CompType>(tuddbs::Utility::generateRandomNumber(0, 5)));
        auto filterValue = filter->add_filtervalue();
        switch (tuddbs::Utility::generateRandomNumber(0, 2)) {
            case 0: {
                IntValue* val = filterValue->mutable_intval();
                val->set_value(tuddbs::Utility::generateRandomNumber(0, 100));
            } break;
            case 1: {
                FloatValue* val = filterValue->mutable_floatval();
                val->set_value(13.37);
            } break;
            case 2: {
                StringValue* val = filterValue->mutable_stringval();
                val->set_value(tuddbs::Utility::generateRandomString());
            } break;
        }
    } else {
        JoinItem* join = item.mutable_joindata();
        ColumnMessage* innerCol = join->mutable_innercolumn();
        ColumnMessage* outerCol = join->mutable_outercolumn();
        ColumnMessage* outputCol = join->mutable_outputcolumn();

        innerCol->set_tabname(tuddbs::Utility::generateRandomString());
        innerCol->set_colname(tuddbs::Utility::generateRandomString());
        innerCol->set_coltype(static_cast<ColumnType>(tuddbs::Utility::generateRandomNumber(0, 4)));

        outerCol->set_tabname(tuddbs::Utility::generateRandomString());
        outerCol->set_colname(tuddbs::Utility::generateRandomString());
        outerCol->set_coltype(static_cast<ColumnType>(tuddbs::Utility::generateRandomNumber(0, 4)));

        outputCol->set_tabname(tuddbs::Utility::generateRandomString());
        outputCol->set_colname(tuddbs::Utility::generateRandomString());
        outputCol->set_coltype(static_cast<ColumnType>(5));

        join->set_joinpredicate(static_cast<CompType>(tuddbs::Utility::generateRandomNumber(0, 5)));
    }

    VLOG("=================================");
    VLOG("     Created a new WorkItem ");
    VLOG("=================================");
    VLOG("Protobuf WorkItem debug info:\n"
         << item.DebugString());
    VLOG("ItemSize: " << item.ByteSizeLong());
    VLOG("=================================");

    return item;
}

void Utility::extractItemsModifyBuffer(void* in_memory, ssize_t in_received_bytes, void* out_message_buffer_start, size_t& out_unprocessed_bytes, CallbackMap* callbacks, bool verbose) {
    ssize_t remaining_bytes = in_received_bytes;
    char* const data_start_ptr = static_cast<char*>(in_memory);
    char* data_ptr = data_start_ptr;

    // Advance until first delimiter. We should <always> find it in the first byte.
    // If not, something went wrong and we need to go back to the drawing board for the messaging protocol.
    while (*reinterpret_cast<uint32_t*>(data_ptr) != TCP_START_DELIM) {
        data_ptr++;
    }

    data_ptr = static_cast<char*>(in_memory);
    if (data_ptr - data_start_ptr > 0) {
        VLOG("[extractItemsModifyBuffer] I expected a delimiter at Byte 0 but did not find it. Data was lost. Exiting.");
        exit(-13);
    }

    // Reinterpret the current data pointer as our meta info struct
    TCPMetaInfo info;
    std::memcpy(&info, data_ptr, sizeof(TCPMetaInfo));

    // As long as complete messages are found in the stream.
    // We break the loop, if payload size + meta size is larger than the remaining bytes in the stream.
    // We can also break, if no bytes are left, i.e. we found a complete package as last object

    while (static_cast<ssize_t>(info.bytesize()) <= remaining_bytes && remaining_bytes > 0) {
        if (verbose) {
            // Debug info
            VLOG("From: " << info.src_uuid << "; To: " << info.tgt_uuid);
            VLOG("Bytes left: " << remaining_bytes << " -- Parsing next message. Type: [" << TcpPackageType_Name(info.package_type) << "] Size: " << info.payload_size);
        }

        // Advance the data pointer past the meta info to the actual payload
        data_ptr += sizeof(TCPMetaInfo);

        // Interpret the payload
        if (callbacks && callbacks->contains(info.package_type)) {
            (*callbacks)[info.package_type](&info, data_ptr, info.payload_size);
        }

        // Advance the data pointer to the next message delimiter.
        data_ptr += info.payload_size;

        // Update our looping boundaries accordingly. We consumed a meta info struct and its corresponding payload.
        remaining_bytes -= info.bytesize();

        // Update the meta info struct pointer
        std::memcpy(&info, data_ptr, sizeof(TCPMetaInfo));
    }

    VLOG("Bytes left after parsing complete messages: " << remaining_bytes << "\n");
    if (remaining_bytes > 0) {
        memmove(out_message_buffer_start, data_ptr, remaining_bytes);
        VLOG("[IncompleteMessage] Moving " << remaining_bytes << " to the start of the buffer.");
    }

    out_unprocessed_bytes = remaining_bytes;
}

}  // namespace tuddbs
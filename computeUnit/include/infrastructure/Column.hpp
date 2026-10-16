#pragma once

#include <numa.h>
#include <stddef.h>

#include <Logger.hpp>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "tsl.hpp"

enum class DataType : uint8_t {
    int8 = 0,
    uint8,
    int16,
    uint16,
    int32,
    uint32,
    int64,
    uint64,
    f32,
    f64,
    position_list,
    bitmask,
    string_enc,
    timestamp,
    unknown
};

struct ColumnNetworkInfo {
    size_t sizeInBytes;
    DataType dataType;
    size_t receivedBytes;

    ColumnNetworkInfo() = default;

    ColumnNetworkInfo(size_t sz, DataType dt) {
        sizeInBytes = sz;
        dataType = dt;
        receivedBytes = 0;
    }

    ColumnNetworkInfo(const ColumnNetworkInfo& other) = default;
    ColumnNetworkInfo& operator=(const ColumnNetworkInfo& other) = default;

    bool checkComplete() const {
        return receivedBytes == sizeInBytes;
    }
};

struct ColumnDictionaryEncoding {
   private:
    std::map<size_t, std::string> dictionaryEncodingNumToString;
    std::unordered_map<std::string, size_t> dictionaryEncodingStringToNum;

    char* serializedDictionary = nullptr;
    size_t serializedDictionarySizeCurrent = 0;
    size_t serializedDictionarySizeTotal = 0;
    size_t dictionaryElementsTotal = 0;

   public:
    mutable std::shared_mutex dictionaryMutex;
    bool isReady = false;
    mutable std::condition_variable_any dictionaryReadyCV;

    size_t encodeValue(const std::string& value) {
        {
            std::shared_lock<std::shared_mutex> lock(dictionaryMutex);
            auto it = dictionaryEncodingStringToNum.find(value);
            if (it != dictionaryEncodingStringToNum.end()) {
                return it->second;
            }
        }
        std::unique_lock<std::shared_mutex> lock(dictionaryMutex);
        // Double-check after acquiring exclusive lock
        auto [it, inserted] = dictionaryEncodingStringToNum.emplace(value, dictionaryEncodingStringToNum.size());
        if (inserted) {
            dictionaryEncodingNumToString.emplace(it->second, value);
        }
        return it->second;
    }

    bool isPresentInDictionary(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);
        return dictionaryEncodingStringToNum.contains(key);
    }

    bool isPresentInDictionary(const std::size_t key) const {
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);
        return dictionaryEncodingNumToString.contains(key);
    }

    size_t getDictionaryValue(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);
        return dictionaryEncodingStringToNum.at(key);
    }

    std::string getDictionaryValue(const size_t key) const {
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);
        return dictionaryEncodingNumToString.at(key);
    }

    std::unordered_set<size_t> getKeysInRange(const std::string& start, const std::string& end) const {
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);
        std::unordered_set<size_t> keys;
        for (const auto& [key, value] : dictionaryEncodingStringToNum) {
            if (key >= start && key <= end) {
                keys.insert(value);
            }
        }
        return keys;
    }

    bool matchesWildcard(const std::string& str, const std::string& pattern, size_t si = 0, size_t pi = 0) {
        while (pi < pattern.size()) {
            if (pattern[pi] == '%') {
                // Skip consecutive %
                while (pi < pattern.size() && pattern[pi] == '%') ++pi;
                if (pi == pattern.size()) return true;  // Trailing % matches everything

                // Try matching % to varying lengths
                for (size_t i = si; i <= str.size(); ++i) {
                    if (matchesWildcard(str, pattern, i, pi)) return true;
                }
                return false;
            } else {
                if (si >= str.size() || str[si] != pattern[pi]) return false;
                ++si;
                ++pi;
            }
        }
        return si == str.size();
    }

    std::unordered_set<size_t> findMatchingValues(const std::string& wildcardPattern) {
        std::unordered_set<size_t> results;
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);
        for (const auto& [key, value] : dictionaryEncodingStringToNum) {
            if (matchesWildcard(key, wildcardPattern)) {
                results.insert(value);
            }
        }
        return results;
    }

    bool checkReady() const {
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);
        return isReady;
    }

    bool setReady() {
        std::unique_lock<std::shared_mutex> lock(dictionaryMutex);
        isReady = true;
        dictionaryReadyCV.notify_all();
        return isReady;
    }

    void waitReady() const {
        std::unique_lock<std::shared_mutex> lock(dictionaryMutex);
        dictionaryReadyCV.wait(lock, [&]() { return isReady; });
    }

    // Returning the approximate size of the dictionary in bytes
    size_t getDictionarySize() const {
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);

        uint64_t size = 0;
        for (const auto& [key, value] : dictionaryEncodingNumToString) {
            size += sizeof(key) + value.length();
        }

        for (const auto& [key, value] : dictionaryEncodingStringToNum) {
            size += key.length() + sizeof(value);
        }

        /* The size of the members should not be incorporated to the dictionary key/value sizes */
        // size += sizeof(dictionaryEncodingNumToString) + sizeof(dictionaryEncodingStringToNum);
        size += dictionaryEncodingNumToString.size() + dictionaryEncodingStringToNum.size();
        return size;
    }

    std::tuple<size_t, size_t, char*> serializeDictionary() {
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);

        size_t size = 0;
        for (const auto& [key, value] : dictionaryEncodingNumToString) {
            size += sizeof(key) + value.length() + sizeof(size_t);
        }

        allocateDictionarySerialization(size, dictionaryEncodingNumToString.size());

        char* tmp = serializedDictionary;
        for (const auto& [key, value] : dictionaryEncodingNumToString) {
            std::memcpy(tmp, &key, sizeof(key));
            tmp += sizeof(key);
            const size_t strLength = value.length();
            std::memcpy(tmp, &strLength, sizeof(size_t));
            tmp += sizeof(size_t);
            std::memcpy(tmp, value.c_str(), strLength);
            tmp += strLength;
        }
        return std::make_tuple(dictionaryEncodingNumToString.size(), size, serializedDictionary);
    }

    bool copyToDictionaryBuffer(char* buffer, const size_t size) {
        std::unique_lock<std::shared_mutex> lock(dictionaryMutex);
        std::memcpy(serializedDictionary, buffer, size);
        serializedDictionarySizeCurrent += size;
        return serializedDictionarySizeCurrent == serializedDictionarySizeTotal;
    }

    void deserializeDictionary() {
        std::unique_lock<std::shared_mutex> lock(dictionaryMutex);
        if (serializedDictionary == nullptr) {
            LOG_ERROR("[Column] No serialized dictionary available. Aborting." << std::endl;)
            return;
        }

        char* tmp = serializedDictionary;
        while (tmp < serializedDictionary + serializedDictionarySizeTotal) {
            uint64_t key;
            std::memcpy(&key, tmp, sizeof(key));
            tmp += sizeof(key);
            size_t strLength;
            std::memcpy(&strLength, tmp, sizeof(size_t));
            tmp += sizeof(size_t);
            std::string value(tmp, strLength);
            dictionaryEncodingNumToString.emplace(key, value);
            dictionaryEncodingStringToNum.emplace(value, key);
            tmp += strLength;
        }

        lock.unlock();

        setReady();

        deallocateDictionarySerialization();
    }

    void allocateDictionarySerialization(const size_t size, const size_t elements) {
        if (serializedDictionary == nullptr) {
            serializedDictionarySizeTotal = size;
            dictionaryElementsTotal = elements;
            char* dict = static_cast<char*>(std::malloc(size));
            if (!dict) {
                std::cerr << "[DictionaryEncoding::allocateDictionarySerialization] Wasn't able to allocate the requested memory!" << std::endl;
                exit(-1);
            }
            serializedDictionary = dict;
        }
    }

    void deallocateDictionarySerialization() {
        std::unique_lock<std::shared_mutex> lock(dictionaryMutex);
        if (serializedDictionary != nullptr) {
            free(serializedDictionary);
            serializedDictionary = nullptr;
            serializedDictionarySizeCurrent = 0;
            serializedDictionarySizeTotal = 0;
            dictionaryElementsTotal = 0;
        }
    }

    void clearDictionary() {
        std::unique_lock<std::shared_mutex> lock(dictionaryMutex);
        dictionaryEncodingNumToString.clear();
        dictionaryEncodingStringToNum.clear();
    }

    std::string getDictionaryAsString(const size_t number = 0) const {
        std::shared_lock<std::shared_mutex> lock(dictionaryMutex);

        std::string dict;
        size_t i = 0;
        for (const auto& [key, value] : dictionaryEncodingNumToString) {
            dict += std::to_string(key) + " -> " + value + "\n";
            if (number > 0 && ++i == number) {
                break;
            }
        }
        return dict;
    }

    ~ColumnDictionaryEncoding() {
        deallocateDictionarySerialization();
    }
};

// TODO support of custom allocators/deallocators
class Column {
   private:
    tsl::executor<tsl::runtime::cpu> executor;

    template <typename T>
    void allocate_aligned_internal(size_t _elements) {
        if (data == nullptr) {
            elements = _elements;
            sizeInBytes = _elements * sizeof(T);
            if (numaNodeId == -1) {
                data = executor.allocate<T>(_elements, 64);
                end = static_cast<void*>(static_cast<char*>(data) + sizeInBytes);
                LOG_DEBUG2("[Column] Allocated " << sizeInBytes << " bytes." << std::endl;)
            } else {
                data = numa_alloc_onnode(sizeInBytes, numaNodeId);
                if (!data) {
                    std::cerr << "[Column::allocate_aligned_internal] Wasn't able to allocate the requested memory!" << std::endl;
                    exit(-1);
                }
                end = static_cast<void*>(static_cast<char*>(data) + sizeInBytes);
                LOG_DEBUG2("[Column] Allocated " << sizeInBytes << " bytes on NUMA node " << numaNodeId << "." << std::endl;)
            }
            if (!isRemote && isComplete) {
                currentEnd = end;
            } else {
                currentEnd = data;
            }

            dataAllocated.store(true);
            dataAllocated.notify_all();
        }
    }

    void allocate_aligned_internal(DataType type, size_t _elements) {
        switch (type) {
            case DataType::int8: {
                allocate_aligned_internal<int8_t>(_elements);
            } break;
            case DataType::uint8: {
                allocate_aligned_internal<uint8_t>(_elements);
            } break;
            case DataType::int16: {
                allocate_aligned_internal<int16_t>(_elements);
            } break;
            case DataType::uint16: {
                allocate_aligned_internal<uint16_t>(_elements);
            } break;
            case DataType::int32: {
                allocate_aligned_internal<int32_t>(_elements);
            } break;
            case DataType::uint32: {
                allocate_aligned_internal<uint32_t>(_elements);
            } break;
            case DataType::int64: {
                allocate_aligned_internal<int64_t>(_elements);
            } break;
            case DataType::string_enc: {
                initDictionary();
            }
                [[fallthrough]];
            case DataType::timestamp:
            case DataType::uint64: {
                allocate_aligned_internal<uint64_t>(_elements);
            } break;
            case DataType::f32: {
                allocate_aligned_internal<float>(_elements);
            } break;
            case DataType::f64: {
                allocate_aligned_internal<double>(_elements);
            } break;
            default: {
                LOG_ERROR("[Column] Error allocating data: Invalid datatype submitted or datatype not supported yet. Nothing was allocated." << std::endl;)
                return;
            }
        }

        std::memset(data, 0, sizeInBytes);
    }

   public:
    size_t remoteConnectionId = 0;

    void* data = nullptr;
    void* currentEnd = nullptr;
    void* end = nullptr;
    DataType datatype = DataType::unknown;
    size_t sizeInBytes = 0;
    size_t elements = 0;

    std::shared_ptr<ColumnDictionaryEncoding> dictionaryEncoding = nullptr;
    std::atomic<bool> dictionaryInitialized{false};

    std::atomic<bool> dataAllocated{false};

    bool isRemote = false;
    bool isComplete = false;

    size_t highestConsecPackArrived = 0;
    std::unordered_map<uint64_t, uint64_t> arrived;

    std::mutex dictMutex;
    std::mutex dataAvailableMutex;
    std::condition_variable dataAvailableCV;

    ssize_t numaNodeId = -1;

    Column() = default;

    explicit Column(const size_t _remoteConnectionId, const DataType _datatype, const size_t _sizeInBytes = 0, const size_t _elements = 0, const bool _isRemote = false, const bool _isComplete = false, const ssize_t _numaNodeId = -1) : remoteConnectionId(_remoteConnectionId), datatype(_datatype), sizeInBytes(_sizeInBytes), elements(_elements), isRemote(_isRemote), isComplete(_isComplete), numaNodeId(_numaNodeId) {
        if (sizeInBytes == 0 && elements == 0) {
            LOG_WARNING("[Column] No size or elements were given. *sizeInBytes* and *elements* were not calculated. Set afterwards!" << std::endl;)
        } else if (elements == 0) {
            calculateElements();
        } else if (sizeInBytes == 0) {
            calculateSizeInBytes();
        }
    }

    void initDictionary(std::shared_ptr<ColumnDictionaryEncoding> encoding = nullptr) {
        std::lock_guard<std::mutex> _lk(dictMutex);
        if (!dictionaryEncoding) {
            dictionaryEncoding = encoding ? encoding : std::make_shared<ColumnDictionaryEncoding>();
            dictionaryInitialized.store(true);
            dictionaryInitialized.notify_all();
        } else if (encoding) {
            LOG_WARNING("[Column::initDictionary] You tried to initialize an already initialized Dictionary Encoding." << std::endl;)
        }
    }

    void calculateElements() {
        switch (datatype) {
            case DataType::int8: {
                elements = sizeInBytes / sizeof(int8_t);
            } break;
            case DataType::uint8: {
                elements = sizeInBytes / sizeof(uint8_t);
            } break;
            case DataType::int16: {
                elements = sizeInBytes / sizeof(int16_t);
            } break;
            case DataType::uint16: {
                elements = sizeInBytes / sizeof(uint16_t);
            } break;
            case DataType::int32: {
                elements = sizeInBytes / sizeof(int32_t);
            } break;
            case DataType::uint32: {
                elements = sizeInBytes / sizeof(uint32_t);
            } break;
            case DataType::int64: {
                elements = sizeInBytes / sizeof(int64_t);
            } break;
            case DataType::uint64:
            case DataType::timestamp:
            case DataType::string_enc: {
                elements = sizeInBytes / sizeof(uint64_t);
            } break;
            case DataType::f32: {
                elements = sizeInBytes / sizeof(float);
            } break;
            case DataType::f64: {
                elements = sizeInBytes / sizeof(double);
            } break;
            default: {
                LOG_WARNING("[Column] Invalid datatype submitted or datatype not supported yet. *elements* was not calculated. Set afterwards!" << std::endl;)
                return;
            }
        }
    }

    void calculateSizeInBytes() {
        switch (datatype) {
            case DataType::int8: {
                sizeInBytes = elements * sizeof(int8_t);
            } break;
            case DataType::uint8: {
                sizeInBytes = elements * sizeof(uint8_t);
            } break;
            case DataType::int16: {
                sizeInBytes = elements * sizeof(int16_t);
            } break;
            case DataType::uint16: {
                sizeInBytes = elements * sizeof(uint16_t);
            } break;
            case DataType::int32: {
                sizeInBytes = elements * sizeof(int32_t);
            } break;
            case DataType::uint32: {
                sizeInBytes = elements * sizeof(uint32_t);
            } break;
            case DataType::int64: {
                sizeInBytes = elements * sizeof(int64_t);
            } break;
            case DataType::uint64:
            case DataType::timestamp:
            case DataType::string_enc: {
                sizeInBytes = elements * sizeof(uint64_t);
            } break;
            case DataType::f32: {
                sizeInBytes = elements * sizeof(float);
            } break;
            case DataType::f64: {
                sizeInBytes = elements * sizeof(double);
            } break;
            default: {
                LOG_WARNING("[Column] Invalid datatype submitted or datatype not supported yet. *sizeInBytes* was not calculated. Set afterwards!" << std::endl;)
                return;
            }
        }
    }

    void setDataPtr(void* newDataPtr, const size_t _sizeInBytes = 0) {
        if (newDataPtr == nullptr) {
            LOG_ERROR("[Column] Invalid data pointer submitted. Aborting." << std::endl;)
            return;
        } else if (data != nullptr) {
            executor.deallocate(data);
        }

        data = newDataPtr;

        if (_sizeInBytes != 0) {
            sizeInBytes = _sizeInBytes;
            calculateElements();
        }

        end = static_cast<void*>(static_cast<char*>(data) + sizeInBytes);

        if (!isRemote && isComplete) {
            currentEnd = end;
        } else {
            currentEnd = data;
        }

        dataAllocated.store(true);
        dataAllocated.notify_all();
    }

    void appendChunk(const size_t offset, const size_t chunkSize, char* remoteData) {
        if (data == nullptr) {
            LOG_WARNING("[Column] !!! Implement allocation handling in append_chunk, aborting." << std::endl;)
            return;
        }
        memcpy(static_cast<char*>(data) + offset, remoteData, chunkSize);
    }

    void advance_end_pointer(const size_t _size) {
        currentEnd = static_cast<void*>(static_cast<char*>(currentEnd) + _size);
        dataAvailableCV.notify_all();
    }

    void allocate() {
        if (data != nullptr) {
            LOG_WARNING("[Column] Data was already allocated. Aborting allocation." << std::endl;)
            return;
        }

        allocate_aligned_internal(datatype, elements);
    }

    void wait_data_allocated() {
        dataAllocated.wait(false);
    }

    ~Column() {
        if (data) {
            if (numaNodeId == -1) {
                executor.deallocate(data);
            } else {
                numa_free(data, sizeInBytes);
            }
        }
    }

    void printFirstTenElements(const std::string& ident) const {
        switch (datatype) {
            case DataType::int8: {
                printFirstTenElements<int8_t>(ident);
            } break;
            case DataType::uint8: {
                printFirstTenElements<uint8_t>(ident);
            } break;
            case DataType::int16: {
                printFirstTenElements<int16_t>(ident);
            } break;
            case DataType::uint16: {
                printFirstTenElements<uint16_t>(ident);
            } break;
            case DataType::int32: {
                printFirstTenElements<int32_t>(ident);
            } break;
            case DataType::uint32: {
                printFirstTenElements<uint32_t>(ident);
            } break;
            case DataType::int64: {
                printFirstTenElements<int64_t>(ident);
            } break;
            case DataType::uint64:
            case DataType::position_list:
            case DataType::timestamp:
            case DataType::string_enc: {
                printFirstTenElements<uint64_t>(ident);
            } break;
            case DataType::f32: {
                printFirstTenElements<float>(ident);
            } break;
            case DataType::f64: {
                printFirstTenElements<double>(ident);
            } break;
            default: {
                LOG_WARNING("[Column] Invalid datatype submitted or datatype not supported yet. *sizeInBytes* was not calculated. Set afterwards!" << std::endl;)
                return;
            }
        }
    }

    template <typename Type>
    void printFirstTenElements(const std::string& ident) const {
        if (elements < 10) {
            LOG_INFO("[Column] All " << elements << " elements of column " << ident << ":" << std::endl;)
        } else {
            LOG_INFO("[Column] First 10 elements of column " << ident << ":" << std::endl;)
        }

        Type* dataPtr = static_cast<Type*>(data);
        for (size_t i = 0; i < 10 && i < elements; ++i) {
            LOG_INFO(+dataPtr[i] << " ";)
        }
        LOG_INFO(std::endl;)
    }
};

template <typename Type>
class View {
   private:
    Type* startPtr = nullptr;
    uint64_t chunkElements = 0;
    std::shared_ptr<Column> column;

    Type* endPtr = nullptr;
    uint64_t lastChunkElements = 0;

   public:
    /**
     * @brief Constructs an View with the specified data pointer.
     * @param data The data pointer.
     */
    View(Type* data, uint64_t _chunkElements, std::shared_ptr<Column> col) noexcept : startPtr(data), column(col) {
        if (data == nullptr) {
            LOG_ERROR("[View] Invalid data pointer submitted. Aborting." << std::endl;)
            return;
        }

        if (column->elements < _chunkElements) {
            chunkElements = column->elements;
            lastChunkElements = column->elements;
        } else {
            chunkElements = _chunkElements;
            lastChunkElements = column->elements % chunkElements == 0 ? _chunkElements : column->elements % chunkElements;
        }
        endPtr = startPtr + chunkElements;
    }

    /**
     * @brief Copy constructor for the View.
     * @param other The View to copy from.
     */
    View(View const& other) noexcept : startPtr(other.startPtr), column(other.column) {
        chunkElements = other.chunkElements;
        endPtr = other.endPtr;
        lastChunkElements = other.lastChunkElements;
    };

    /**
     * @brief Move constructor for the View.
     * @param other The View to move from.
     */
    View(View&& other) noexcept : startPtr(std::exchange(other.startPtr, nullptr)), chunkElements(other.chunkElements), column(other.column), endPtr(std::exchange(other.endPtr, nullptr)), lastChunkElements(other.lastChunkElements) {}

    /**
     * @brief Copy assignment operator for the View.
     * @param other The View to copy from.
     * @return Reference to the copied View.
     */
    View& operator=(View const& other) noexcept {
        if (this != &other) {
            startPtr = other.startPtr;
            chunkElements = other.chunkElements;
            column = other.column;
            endPtr = other.endPtr;
            lastChunkElements = other.lastChunkElements;
        }
        return *this;
    }

    /**
     * @brief Move assignment operator for the View.
     * @param other The View to move from.
     * @return Reference to the moved View.
     */
    View& operator=(View&& other) noexcept {
        if (this != &other) {
            startPtr = std::exchange(other.startPtr, nullptr);
            chunkElements = other.chunkElements;
            column = other.column;
            endPtr = std::exchange(other.endPtr, nullptr);
            lastChunkElements = other.lastChunkElements;
        }
        return *this;
    }

    /**
     * @brief Destructor for the view.
     */
    virtual ~View() noexcept {}

    /**
     * @brief Pre-increment operator for the view.
     * @return Reference to the incremented view.
     */
    auto operator++() noexcept -> View& {
        Type* newStartPtr = startPtr + chunkElements;
        Type* newEndPtr = endPtr + chunkElements;
        if (newStartPtr < column->end) {
            startPtr = newStartPtr;
            if (newEndPtr > column->end) {
                endPtr = reinterpret_cast<Type*>(column->end);
            } else {
                endPtr = newEndPtr;
            }
        } else {
            endPtr = reinterpret_cast<Type*>(column->end);
        }
        waitDataReady(newStartPtr);
        return *this;
    }

    /**
     * @brief Post-increment operator for the view.
     * @return Copy of the view before incrementing.
     */
    auto operator++(int) noexcept -> View {
        auto tmp = *this;
        Type* newStartPtr = startPtr + chunkElements;
        Type* newEndPtr = endPtr + chunkElements;
        if (newStartPtr < column->end) {
            startPtr = newStartPtr;
            if (newEndPtr > column->end) {
                endPtr = reinterpret_cast<Type*>(column->end);
            } else {
                endPtr = newEndPtr;
            }
        } else {
            endPtr = reinterpret_cast<Type*>(column->end);
        }
        waitDataReady(newStartPtr);
        return tmp;
    }

    /**
     * @brief Compound assignment operator for the view.
     * @param i The value to add to the view.
     * @return Reference to the updated view.
     */
    auto operator+=(size_t i) noexcept -> View& {
        Type* newStartPtr = startPtr + chunkElements * i;
        Type* newEndPtr = endPtr + chunkElements * i;

        if (newStartPtr < column->end) {
            startPtr = newStartPtr;
            if (newEndPtr > column->end) {
                endPtr = reinterpret_cast<Type*>(column->end);
            } else {
                endPtr = newEndPtr;
            }
        } else {
            endPtr = reinterpret_cast<Type*>(column->end);
        }
        waitDataReady(newStartPtr);
        return *this;
    }

    bool isLastChunk() const {
        return endPtr == column->end;
    }

    void waitDataReady(Type* data) const {
        std::unique_lock<std::mutex> lock(column->dataAvailableMutex);
        column->dataAvailableCV.wait(lock, [&]() { return column->currentEnd == column->end || data + getNumberOfElements() <= column->currentEnd; });
    }

    Type* begin() const {
        waitDataReady(startPtr);
        return startPtr;
    }

    Type* end() const {
        return endPtr;
    }

    uint64_t getNumberOfElements() const {
        if (isLastChunk()) {
            return lastChunkElements;
        }
        return chunkElements;
    }
};
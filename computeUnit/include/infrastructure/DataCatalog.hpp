#pragma once

#include <condition_variable>
#include <cstdint>
#include <infrastructure/Column.hpp>
#include <map>
#include <shared_mutex>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>


enum class CatalogCommunicationCode : uint8_t {
    send_column_info = 0xA0,
    receive_column_info,
    fetch_column_as_stream,
    receive_column_as_stream,
    fetch_pseudo_pax_stream,
    receive_pseudo_pax_stream,
    fetch_dictionary_encoding,
    receive_dictionary_encoding,
    reconfigure_chunk_size,
    ack_reconfigure_chunk_size,
    clear_catalog,
    ack_clear_catalog
};

class DataCatalog {
   private:
    DataCatalog();
    
    public:
    static DataCatalog& getInstance();
    
    DataCatalog(DataCatalog const&) = delete;
    void operator=(DataCatalog const&) = delete;
    ~DataCatalog();
    
    void clearCatalog(bool verbose = true);
    
    std::shared_ptr<Column> getColumnByName(const std::string& tableName, const std::string& columnNamee) const;

    std::shared_ptr<Column> addColumn(const std::string& tableName, const std::string& columnName, std::shared_ptr<Column> col);
    std::shared_ptr<Column> addColumn(const std::string& tableName, const std::string& columnName, const ColumnNetworkInfo& col, const size_t conId, const bool isRemote = false, const bool isComplete = false);

    int removeColumn(const std::string& tableName, const std::string& columnName);
    int dropTable(const std::string& tableName);

    size_t getColumnCount() const;
    size_t getTotalCatalogSize() const;
    size_t getTableSize(const std::string& tableName) const;

   private:
    mutable std::shared_mutex catalogMutex;

    std::unordered_map<std::string, std::unordered_map<std::string, std::shared_ptr<Column>>> tables;
};
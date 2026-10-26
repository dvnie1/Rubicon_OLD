#include <infrastructure/DataCatalog.hpp>
#include <iostream>

DataCatalog::DataCatalog() {}

DataCatalog& DataCatalog::getInstance() {
    static DataCatalog instance;
    return instance;
}

std::shared_ptr<Column> DataCatalog::getColumnByName(const std::string& tableName, const std::string& columnName) const {
    std::shared_lock<std::shared_mutex> lk(catalogMutex);
    if (tables.contains(tableName)) {
        if (tables.at(tableName).contains(columnName)) {
            return tables.at(tableName).at(columnName);
        }
    }

    return nullptr;
}

size_t DataCatalog::computeTotalCatalogSizeUnlocked() const {
    size_t totalSize = 0;
    for (const auto& table : tables) {
        for (const auto& col : table.second) {
            if (!col.second || col.second->isRemote) {
                continue;
            }
            totalSize += col.second->sizeInBytes;
            if (col.second->datatype == DataType::string_enc && col.second->dictionaryEncoding != nullptr) {
                totalSize += col.second->dictionaryEncoding->getDictionarySize();
            }
        }
    }
    return totalSize;
}

std::shared_ptr<Column> DataCatalog::addColumn(const std::string& tableName, const std::string& columnName, std::shared_ptr<Column> col) {
    std::unique_lock<std::shared_mutex> lk(catalogMutex);
    if (!tables.contains(tableName)) {
        tables.emplace(tableName, std::unordered_map<std::string, std::shared_ptr<Column>>());
    }

    if (tables[tableName].contains(columnName)) {
        LOG_WARNING("[DataCatalog] Cannot add column with name \"" << columnName << "\" to table \"" << tableName << "\". Column-Identifier already exists for this table." << std::endl;)
        return nullptr;
    }

    tables[tableName].emplace(columnName, col);
    LOG_DEBUG1("[DataCatalog] Added column with name \"" << columnName << "\" to table \"" << tableName << "\"." << std::endl;)
    return col;
}

std::shared_ptr<Column> DataCatalog::addColumn(const std::string& tableName, const std::string& columnName, const ColumnNetworkInfo& col, const size_t conId, const bool isRemote, const bool isComplete) {
    auto column = std::make_shared<Column>(conId, col.dataType, col.sizeInBytes, 0, isRemote, isComplete);
    return addColumn(tableName, columnName, column);
}

/**
 * @brief Removing a column corresponding to the given name from the catalog. Pay attention: the column, as it is a shared_ptr, can be still in use by other parts of the program. This is currently intended behaviour.
 *
 * @param name Ident of the column to remove
 * @return int The number of elements erased from the map -> 1 if the column was removed and 0 if it was not found
 */
int DataCatalog::removeColumn(const std::string& tableName, const std::string& columnName) {
    std::lock_guard<std::shared_mutex> lk(catalogMutex);
    if (tables.contains(tableName) && tables[tableName].contains(columnName)) {
        size_t cur = computeTotalCatalogSizeUnlocked();
        peakCatalogBytes.store(std::max(peakCatalogBytes.load(std::memory_order_relaxed), cur), std::memory_order_relaxed);
        int ret = tables[tableName].erase(columnName);
        if (tables[tableName].empty()) {
            tables.erase(tableName);
        }
        return ret;
    }

    LOG_ERROR("[DataCatalog] Error - cannot remove column with name \"" << columnName << "\" from table \"" << tableName << "\". Column-Identifier does not exist for this table." << std::endl;)
    return 0;
}

int DataCatalog::dropTable(const std::string& tableName) {
    std::lock_guard<std::shared_mutex> lk(catalogMutex);
    if (tables.contains(tableName)) {
        size_t cur = computeTotalCatalogSizeUnlocked();
        peakCatalogBytes.store(std::max(peakCatalogBytes.load(std::memory_order_relaxed), cur), std::memory_order_relaxed);
        int ret = tables[tableName].size();
        tables.erase(tableName);
        return ret;
    }

    LOG_ERROR("[DataCatalog] Error - cannot drop table with name \"" << tableName << "\". Table does not exist." << std::endl;)
    return 0;
}

void DataCatalog::clearCatalog(bool verbose) {
    if (verbose) {
        LOG_INFO("[DataCatalog] Clearing catalog." << std::endl;)
    }

    std::lock_guard<std::shared_mutex> lk(catalogMutex);
    tables.clear();
}

size_t DataCatalog::getColumnCount() const {
    std::shared_lock<std::shared_mutex> lk(catalogMutex);
    size_t count = 0;
    for (auto& table : tables) {
        count += table.second.size();
    }
    return count;
}

size_t DataCatalog::getTotalCatalogSize() const {
    std::shared_lock<std::shared_mutex> lk(catalogMutex);
    return computeTotalCatalogSizeUnlocked();
}

size_t DataCatalog::consumePeakCatalogSize() const {
    std::shared_lock<std::shared_mutex> lk(catalogMutex);
    size_t current = computeTotalCatalogSizeUnlocked();
    size_t prev_peak = peakCatalogBytes.exchange(current, std::memory_order_relaxed);
    return std::max(current, prev_peak);
}

size_t DataCatalog::getTableSize(const std::string& tableName) const {
    std::shared_lock<std::shared_mutex> lk(catalogMutex);
    if (tables.contains(tableName)) {
        size_t totalSize = 0;
        for (auto& col : tables.at(tableName)) {
            if (!col.second || col.second->isRemote) {
                continue;
            }
            totalSize += col.second->sizeInBytes;
            if (col.second->datatype == DataType::string_enc && col.second->dictionaryEncoding != nullptr) {
                totalSize += col.second->dictionaryEncoding->getDictionarySize();
            }
        }
        return totalSize;
    }
    return 0;
}

DataCatalog::~DataCatalog() {
    clearCatalog(false);
}

#pragma once

#include <algorithm>
#include <fstream>
#include <functional>
#include <iostream>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "QueryPlan.pb.h"
#include "WorkItem.pb.h"
#include "../../tcp_stuff/include/TCPServer.hpp"

namespace plan {

// ============================================================================
// Validation Result
// ============================================================================

struct ValidationResult {
    bool valid = true;
    std::vector<std::string> errors;
    std::vector<std::string> warnings;

    void addError(const std::string& msg) {
        valid = false;
        errors.push_back(msg);
    }

    void addWarning(const std::string& msg) {
        warnings.push_back(msg);
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << (valid ? "Validation PASSED" : "Validation FAILED") << "\n";

        if (!errors.empty()) {
            oss << "\nErrors:\n";
            for (const auto& e : errors) {
                oss << "  - " << e << "\n";
            }
        }

        if (!warnings.empty()) {
            oss << "\nWarnings:\n";
            for (const auto& w : warnings) {
                oss << "  - " << w << "\n";
            }
        }

        return oss.str();
    }
};

// ============================================================================
// Helper functions to extract columns from WorkItem
// ============================================================================

inline std::string columnFullName(const ColumnMessage& col) {
    return col.tabname() + "." + col.colname();
}

// Known postfixes that operators add to output column names
// Add more postfixes here as needed
inline const std::vector<std::string>& getKnownPostfixes() {
    static const std::vector<std::string> postfixes = {
        "_i",        // inner (join)
        "_o",        // outer (join)
        "_idx",      // index
        "_idx_ext",  // extended index
        "_agg",      // aggregation result
        "_sorted",   // sort output
        "_pos",      // position list
        "_mask",     // bitmask
        "_result",   // result
        "_out",      // output
        "_in",       // input
        "_left",     // left side
        "_right",    // right side
        "_l",        // left (short)
        "_r",        // right (short)
    };
    return postfixes;
}

// Strip known postfixes from column name for comparison
inline std::string stripPostfix(const std::string& colName) {
    std::string result = colName;

    // Try to strip each known postfix (longest first for proper matching)
    for (const auto& postfix : getKnownPostfixes()) {
        if (result.size() > postfix.size() &&
            result.compare(result.size() - postfix.size(), postfix.size(), postfix) == 0) {
            result = result.substr(0, result.size() - postfix.size());
            // Don't break - allow stripping multiple postfixes if needed
            // (e.g., "_idx_ext" should be stripped as whole, but also handle "_idx" + "_ext")
        }
    }

    return result;
}

// Get base column name (table.column with postfixes stripped)
inline std::string columnBaseName(const ColumnMessage& col) {
    return col.tabname() + "." + stripPostfix(col.colname());
}

// Check if column is from base data (should skip validation)
inline bool isBaseColumn(const ColumnMessage& col) {
    return col.has_isbase() && col.isbase();
}

// Get all input columns that a WorkItem consumes
inline std::vector<const ColumnMessage*> getInputColumns(const WorkItem& item) {
    std::vector<const ColumnMessage*> inputs;

    switch (item.opData_case()) {
        case WorkItem::kFilterData:
            if (item.filterdata().has_inputcolumn()) {
                inputs.push_back(&item.filterdata().inputcolumn());
            }
            break;

        case WorkItem::kJoinData:
            if (item.joindata().has_innercolumn()) {
                inputs.push_back(&item.joindata().innercolumn());
            }
            if (item.joindata().has_outercolumn()) {
                inputs.push_back(&item.joindata().outercolumn());
            }
            break;

        case WorkItem::kAggData:
            if (item.aggdata().has_inputcolumn()) {
                inputs.push_back(&item.aggdata().inputcolumn());
            }
            break;

        case WorkItem::kSortData:
            for (const auto& col : item.sortdata().inputcolumns()) {
                inputs.push_back(&col);
            }
            if (item.sortdata().has_existingindex()) {
                inputs.push_back(&item.sortdata().existingindex());
            }
            break;

        case WorkItem::kMapData:
            if (item.mapdata().has_inputcolumn()) {
                inputs.push_back(&item.mapdata().inputcolumn());
            }
            if (item.mapdata().has_partnercolumn()) {
                inputs.push_back(&item.mapdata().partnercolumn());
            }
            break;

        case WorkItem::kMaterializeData:
            if (item.materializedata().has_indexcolumn()) {
                inputs.push_back(&item.materializedata().indexcolumn());
            }
            if (item.materializedata().has_filtercolumn()) {
                inputs.push_back(&item.materializedata().filtercolumn());
            }
            break;

        case WorkItem::kSetData:
            if (item.setdata().has_innercolumn()) {
                inputs.push_back(&item.setdata().innercolumn());
            }
            if (item.setdata().has_outercolumn()) {
                inputs.push_back(&item.setdata().outercolumn());
            }
            break;

        case WorkItem::kFetchData:
            if (item.fetchdata().has_inputcolumn()) {
                inputs.push_back(&item.fetchdata().inputcolumn());
            }
            break;

        case WorkItem::kMultiGroupData:
            for (const auto& col : item.multigroupdata().groupcolumns()) {
                inputs.push_back(&col);
            }
            if (item.multigroupdata().has_aggregationcolumn()) {
                inputs.push_back(&item.multigroupdata().aggregationcolumn());
            }
            break;

        case WorkItem::kResultData:
            for (const auto& col : item.resultdata().resultcolumns()) {
                inputs.push_back(&col);
            }
            if (item.resultdata().has_resultindex()) {
                inputs.push_back(&item.resultdata().resultindex());
            }
            break;

        default:
            break;
    }

    return inputs;
}

// Get all output columns that a WorkItem produces
inline std::vector<const ColumnMessage*> getOutputColumns(const WorkItem& item) {
    std::vector<const ColumnMessage*> outputs;

    switch (item.opData_case()) {
        case WorkItem::kFilterData:
            if (item.filterdata().has_outputcolumn()) {
                outputs.push_back(&item.filterdata().outputcolumn());
            }
            break;

        case WorkItem::kJoinData:
            if (item.joindata().has_outputcolumn()) {
                outputs.push_back(&item.joindata().outputcolumn());
            }
            break;

        case WorkItem::kAggData:
            if (item.aggdata().has_outputcolumn()) {
                outputs.push_back(&item.aggdata().outputcolumn());
            }
            break;

        case WorkItem::kSortData:
            if (item.sortdata().has_indexoutput()) {
                outputs.push_back(&item.sortdata().indexoutput());
            }
            break;

        case WorkItem::kMapData:
            if (item.mapdata().has_outputcolumn()) {
                outputs.push_back(&item.mapdata().outputcolumn());
            }
            break;

        case WorkItem::kMaterializeData:
            if (item.materializedata().has_outputcolumn()) {
                outputs.push_back(&item.materializedata().outputcolumn());
            }
            break;

        case WorkItem::kSetData:
            if (item.setdata().has_outputcolumn()) {
                outputs.push_back(&item.setdata().outputcolumn());
            }
            break;

        case WorkItem::kMultiGroupData:
            if (item.multigroupdata().has_outputindex()) {
                outputs.push_back(&item.multigroupdata().outputindex());
            }
            if (item.multigroupdata().has_outputclusters()) {
                outputs.push_back(&item.multigroupdata().outputclusters());
            }
            if (item.multigroupdata().has_aggregationresultcolumn()) {
                outputs.push_back(&item.multigroupdata().aggregationresultcolumn());
            }
            break;

        default:
            break;
    }

    return outputs;
}

// Get operator name as string
inline std::string getOperatorName(const WorkItem& item) {
    if (!item.has_operatorid()) return "UNKNOWN";
    switch (item.operatorid()) {
        case OP_FILTER:
            return "FILTER";
        case OP_IDXSCAN:
            return "IDXSCAN";
        case OP_NLJ:
            return "NLJ";
        case OP_HASHJOIN:
            return "HASHJOIN";
        case OP_MERGEJOIN:
            return "MERGEJOIN";
        case OP_AGGREGATE:
            return "AGGREGATE";
        case OP_SETOPERATION:
            return "SETOPERATION";
        case OP_SORT:
            return "SORT";
        case OP_MAP:
            return "MAP";
        case OP_MATERIALIZE:
            return "MATERIALIZE";
        case OP_GROUPBY:
            return "GROUPBY";
        case OP_RESULT:
            return "RESULT";
        default:
            return "UNKNOWN";
    }
}

// ============================================================================
// Plan DAG Class
// ============================================================================

using BasColMap = std::unordered_map<std::string, std::vector<uint32_t>>;
using BasColTypeMap = std::unordered_map<std::string, ColumnType>;

class PlanDAG {
   public:
    using ItemMap = std::unordered_map<uint32_t, const WorkItem*>;
    using AdjList = std::unordered_map<uint32_t, std::vector<uint32_t>>;

   private:
    uint32_t planId_;
    ItemMap items_;
    AdjList children_;  // itemId -> items that depend on it
    AdjList parents_;   // itemId -> items it depends on
    std::optional<uint32_t> rootId_;
    std::unordered_set<uint32_t> leaves_;
    BasColMap baseColumns_;
    BasColTypeMap baseColumnTypes_;
    
    QueryPlan plan_;
    uint64_t sourceUuid_;
    uint64_t targetUuid_;

   public:
    PlanDAG(QueryPlan&& plan, tuddbs::TCPMetaInfo* meta = nullptr) {
        planId_ = plan.planid();
        addItems(plan.planitems());
        plan_ = std::move(plan);
        sourceUuid_ = meta ? meta->src_uuid : 0;
        targetUuid_ = meta ? meta->tgt_uuid : 0;
    }

    // Add a WorkItem to the DAG (stores pointer, does not copy)
    void addItem(const WorkItem& item) {
        uint32_t id = item.itemid();

        // Store dependencies
        std::vector<uint32_t> deps;
        for (int i = 0; i < item.dependson_size(); ++i) {
            deps.push_back(item.dependson(i));
            children_[item.dependson(i)].push_back(id);
        }
        parents_[id] = std::move(deps);

        items_[id] = &item;
    }

    // Add all items from a repeated field (e.g., from a Plan message)
    template <typename Container>
    void addItems(const Container& items) {
        for (const auto& item : items) {
            addItem(item);
        }
    }

    // Build the DAG structure (call after adding all items)
    ValidationResult build() {
        ValidationResult result;

        leaves_.clear();
        rootId_.reset();
        baseColumns_.clear();
        baseColumnTypes_.clear();

        std::vector<uint32_t> resultItems;
        std::vector<uint32_t> nodesWithNoChildren;

        for (const auto& [id, item] : items_) {
            // Check for leaves (no dependencies)
            if (item->dependson_size() == 0) {
                leaves_.insert(id);
            }

            // Check for ResultItem - this is the logical root
            if (item->opData_case() == WorkItem::kResultData) {
                resultItems.push_back(id);
            }

            // Track nodes with no children (nothing depends on them)
            if (children_.find(id) == children_.end() || children_.at(id).empty()) {
                nodesWithNoChildren.push_back(id);
            }

            // Collect all base columns and which items use them
            for (const auto* col : getInputColumns(*item)) {
                if (isBaseColumn(*col)) {
                    const std::string colName = columnFullName(*col);
                    baseColumns_[colName].push_back(id);

                    if (!baseColumnTypes_.contains(colName)) {
                        baseColumnTypes_[colName] = col->coltype();
                    }
                }
            }
        }

        // Determine root
        if (resultItems.empty()) {
            // No ResultItem found - fall back to nodes with no children
            if (nodesWithNoChildren.empty()) {
                result.addError("No root found - no ResultItem and possible cycle in the graph");
            } else if (nodesWithNoChildren.size() > 1) {
                std::ostringstream oss;
                oss << "No ResultItem found. Multiple terminal nodes: ";
                for (size_t i = 0; i < nodesWithNoChildren.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << nodesWithNoChildren[i];
                }
                result.addWarning(oss.str());
                // Pick one arbitrarily (first one)
                rootId_ = nodesWithNoChildren[0];
            } else {
                result.addWarning("No ResultItem found, using terminal node " +
                                  std::to_string(nodesWithNoChildren[0]) + " as root");
                rootId_ = nodesWithNoChildren[0];
            }
        } else if (resultItems.size() > 1) {
            std::ostringstream oss;
            oss << "Multiple ResultItems found: ";
            for (size_t i = 0; i < resultItems.size(); ++i) {
                if (i > 0) oss << ", ";
                oss << resultItems[i];
            }
            oss << ". Using first one (" << resultItems[0] << ") as root.";
            result.addWarning(oss.str());
            rootId_ = resultItems[0];
        } else {
            rootId_ = resultItems[0];
        }

        if (leaves_.empty()) {
            result.addError("No leaves found - possible cycle in the graph");
        }

        return result;
    }

    // Validate that all paths lead to the root
    ValidationResult validatePaths() const {
        ValidationResult result;

        if (!rootId_) {
            result.addError("No root defined - call build() first");
            return result;
        }

        // BFS from root backwards
        std::unordered_set<uint32_t> reachableFromRoot;
        std::queue<uint32_t> queue;
        queue.push(*rootId_);
        reachableFromRoot.insert(*rootId_);

        while (!queue.empty()) {
            uint32_t current = queue.front();
            queue.pop();

            if (parents_.count(current)) {
                for (uint32_t dep : parents_.at(current)) {
                    if (reachableFromRoot.find(dep) == reachableFromRoot.end()) {
                        reachableFromRoot.insert(dep);
                        queue.push(dep);
                    }
                }
            }
        }

        // Check for unreachable nodes
        for (const auto& [id, _] : items_) {
            if (reachableFromRoot.find(id) == reachableFromRoot.end()) {
                result.addError("Item " + std::to_string(id) +
                                " is not on any path to root " +
                                std::to_string(*rootId_));
            }
        }

        // Check forward reachability
        for (const auto& [id, _] : items_) {
            if (id == *rootId_) continue;

            std::unordered_set<uint32_t> visited;
            if (!canReachRoot(id, visited)) {
                result.addError("Item " + std::to_string(id) +
                                " cannot reach root " + std::to_string(*rootId_));
            }
        }

        return result;
    }

    // Validate input/output column matching
    // - Only checks intermediate columns (isBase = false)
    // - Compares base names (strips postfixes like _i, _o, _agg, etc.)
    ValidationResult validateColumnMatching() const {
        ValidationResult result;

        // Build map of output column base names -> producer items
        // We store both exact names and base names for flexible matching
        std::unordered_map<std::string, std::vector<uint32_t>> columnProducersByBaseName;
        std::unordered_map<std::string, std::vector<uint32_t>> columnProducersByExactName;

        for (const auto& [id, item] : items_) {
            for (const auto* col : getOutputColumns(*item)) {
                std::string exactName = columnFullName(*col);
                std::string baseName = columnBaseName(*col);

                columnProducersByExactName[exactName].push_back(id);
                columnProducersByBaseName[baseName].push_back(id);
            }
        }

        // Check each item's inputs
        for (const auto& [id, item] : items_) {
            auto inputs = getInputColumns(*item);
            std::unordered_set<uint32_t> deps;
            for (int i = 0; i < item->dependson_size(); ++i) {
                deps.insert(item->dependson(i));
            }

            for (const auto* inputCol : inputs) {
                // Skip base data columns - they come from external tables
                if (isBaseColumn(*inputCol)) {
                    continue;
                }

                std::string exactName = columnFullName(*inputCol);
                std::string baseName = columnBaseName(*inputCol);

                // First try exact match, then base name match
                std::vector<uint32_t>* producers = nullptr;
                std::string matchedName;

                auto exactIt = columnProducersByExactName.find(exactName);
                if (exactIt != columnProducersByExactName.end()) {
                    producers = &exactIt->second;
                    matchedName = exactName;
                } else {
                    auto baseIt = columnProducersByBaseName.find(baseName);
                    if (baseIt != columnProducersByBaseName.end()) {
                        producers = &baseIt->second;
                        matchedName = baseName + " (base name match)";
                    }
                }

                if (!producers) {
                    // Column not produced by any plan item
                    result.addWarning("Item " + std::to_string(id) +
                                      " uses intermediate column '" + exactName +
                                      "' which is not produced by any item in the plan");
                    continue;
                }

                // Check if producer is in dependencies (direct or transitive)
                bool foundInDeps = false;
                for (uint32_t producer : *producers) {
                    if (deps.count(producer) || isTransitiveDependency(id, producer)) {
                        foundInDeps = true;
                        break;
                    }
                }

                if (!foundInDeps && !producers->empty()) {
                    std::ostringstream oss;
                    oss << "Item " << id << " uses column '" << exactName
                        << "' (matched as " << matchedName << ") produced by item(s) ";
                    for (size_t i = 0; i < producers->size(); ++i) {
                        if (i > 0) oss << ", ";
                        oss << (*producers)[i];
                    }
                    oss << " but doesn't depend on them";
                    result.addError(oss.str());
                }
            }
        }

        return result;
    }

    // Detect cycles
    ValidationResult detectCycles() const {
        ValidationResult result;

        std::unordered_set<uint32_t> visited;
        std::unordered_set<uint32_t> recursionStack;
        std::vector<uint32_t> cyclePath;

        for (const auto& [id, _] : items_) {
            if (hasCycleDFS(id, visited, recursionStack, cyclePath)) {
                std::ostringstream oss;
                oss << "Cycle detected involving items: ";
                for (size_t i = 0; i < cyclePath.size(); ++i) {
                    if (i > 0) oss << " -> ";
                    oss << cyclePath[i];
                }
                result.addError(oss.str());
                break;
            }
        }

        return result;
    }

    // Full validation
    ValidationResult validate() const {
        ValidationResult result;

        auto cycleResult = detectCycles();
        result.errors.insert(result.errors.end(),
                             cycleResult.errors.begin(),
                             cycleResult.errors.end());
        result.warnings.insert(result.warnings.end(),
                               cycleResult.warnings.begin(),
                               cycleResult.warnings.end());

        if (!cycleResult.valid) {
            result.valid = false;
            return result;
        }

        auto pathResult = validatePaths();
        result.errors.insert(result.errors.end(),
                             pathResult.errors.begin(),
                             pathResult.errors.end());
        result.warnings.insert(result.warnings.end(),
                               pathResult.warnings.begin(),
                               pathResult.warnings.end());

        auto colResult = validateColumnMatching();
        result.errors.insert(result.errors.end(),
                             colResult.errors.begin(),
                             colResult.errors.end());
        result.warnings.insert(result.warnings.end(),
                               colResult.warnings.begin(),
                               colResult.warnings.end());

        result.valid = result.errors.empty();
        return result;
    }

    // Topological sort for execution order
    std::vector<uint32_t> topologicalSort() const {
        std::vector<uint32_t> order;
        std::unordered_map<uint32_t, int> inDegree;

        for (const auto& [id, _] : items_) {
            inDegree[id] = 0;
        }

        for (const auto& [id, deps] : parents_) {
            inDegree[id] = static_cast<int>(deps.size());
        }

        std::queue<uint32_t> queue;
        for (const auto& [id, degree] : inDegree) {
            if (degree == 0) {
                queue.push(id);
            }
        }

        while (!queue.empty()) {
            uint32_t current = queue.front();
            queue.pop();
            order.push_back(current);

            if (children_.count(current)) {
                for (uint32_t child : children_.at(current)) {
                    if (--inDegree[child] == 0) {
                        queue.push(child);
                    }
                }
            }
        }

        return order;
    }

    // Iterate items in topological order with a callback
    void forEachInOrder(std::function<void(const WorkItem&)> callback) const {
        for (uint32_t id : topologicalSort()) {
            if (auto* item = getItem(id)) {
                callback(*item);
            }
        }
    }

    uint32_t getPlanId() const { return planId_; }

    // Accessors
    const WorkItem* getItem(uint32_t id) const {
        auto it = items_.find(id);
        return it != items_.end() ? it->second : nullptr;
    }

    const ItemMap& getAllItems() const {
        return items_;
    }

    const BasColMap& getBaseColumns() const {
        return baseColumns_;
    }

    const BasColTypeMap& getBaseColumnTypes() const {
        return baseColumnTypes_;
    }

    QueryPlan& getPlan() {
        return plan_;
    }

    const QueryPlan& getPlan() const {
        return plan_;
    }

    uint64_t getSourceUuid() const {
        return sourceUuid_;
    }

    uint64_t getTargetUuid() const {
        return targetUuid_;
    }

    std::optional<uint32_t> getRoot() const {
        return rootId_;
    }

    const std::unordered_set<uint32_t>& getLeaves() const {
        return leaves_;
    }

    size_t size() const {
        return items_.size();
    }

    std::vector<uint32_t> getChildren(uint32_t id) const {
        auto it = children_.find(id);
        return it != children_.end() ? it->second : std::vector<uint32_t>{};
    }

    std::vector<uint32_t> getParents(uint32_t id) const {
        auto it = parents_.find(id);
        return it != parents_.end() ? it->second : std::vector<uint32_t>{};
    }

    // Print DAG structure
    void print(std::ostream& os = std::cout) const {
        os << "=== Plan DAG ===\n";
        os << "Items: " << items_.size() << "\n";
        os << "Root: " << (rootId_ ? std::to_string(*rootId_) : "none") << "\n";
        os << "Leaves: ";
        bool first = true;
        for (uint32_t leaf : leaves_) {
            if (!first) os << ", ";
            first = false;
            os << leaf;
        }
        os << "\n\n";

        os << "Topological order:\n  ";
        auto order = topologicalSort();
        for (size_t i = 0; i < order.size(); ++i) {
            if (i > 0) os << " -> ";
            os << order[i];
            if (auto* item = getItem(order[i])) {
                os << "(" << getOperatorName(*item) << ")";
            }
        }
        os << "\n\n";

        os << "Dependencies:\n";
        for (uint32_t id : order) {
            if (auto* item = getItem(id)) {
                os << "  [" << id << "] " << getOperatorName(*item) << " <- ";
                if (item->dependson_size() == 0) {
                    os << "(leaf)";
                } else {
                    for (int i = 0; i < item->dependson_size(); ++i) {
                        if (i > 0) os << ", ";
                        os << item->dependson(i);
                    }
                }
                os << "\n";
            }
        }
    }

    // Export DAG as Graphviz DOT format
    void exportDot(std::ostream& os, const std::string& graphName = "PlanDAG") const {
        os << "digraph " << graphName << " {\n";
        os << "    rankdir=BT;\n";  // Bottom to top (leaves at bottom, root at top)
        os << "    node [shape=record, fontname=\"Helvetica\"];\n";
        os << "    edge [fontname=\"Helvetica\", fontsize=10];\n";
        os << "\n";

        // Define base column nodes
        os << "    // Base column nodes\n";
        for (const auto& [colName, _] : baseColumns_) {
            std::string nodeId = baseColumnNodeId(colName);
            os << "    " << nodeId << " [label=\"" << escapeLabel(colName) << "\"";
            os << ", style=filled, fillcolor=\"#FFD700\"";  // Gold for base columns
            os << ", shape=ellipse";                        // Different shape to distinguish from operators
            os << "];\n";
        }
        os << "\n";

        // Define operator nodes with labels
        os << "    // Operator nodes\n";
        for (const auto& [id, item] : items_) {
            os << "    node_" << id << " [label=\"{";
            os << id << " | " << getOperatorName(*item);

            // Add operator-specific details
            switch (item->opData_case()) {
                case WorkItem::kResultData:
                    os << " | " << escapeLabel(item->resultdata().filename());
                    break;
                case WorkItem::kFilterData:
                    if (item->filterdata().has_filtertype()) {
                        os << " | " << compTypeToString(item->filterdata().filtertype());
                    }
                    break;
                case WorkItem::kJoinData:
                    if (item->joindata().has_joinpredicate()) {
                        os << " | " << compTypeToString(item->joindata().joinpredicate());
                    }
                    break;
                case WorkItem::kAggData:
                    if (item->aggdata().has_aggfunc()) {
                        os << " | " << aggFuncToString(item->aggdata().aggfunc());
                    }
                    break;
                case WorkItem::kSetData:
                    if (item->setdata().has_operation()) {
                        os << " | " << relOpToString(item->setdata().operation());
                    }
                    break;
                default:
                    break;
            }

            // Add input columns (only intermediate, not base)
            auto inputs = getInputColumns(*item);
            std::vector<const ColumnMessage*> intermediateInputs;
            for (const auto* col : inputs) {
                if (!isBaseColumn(*col)) {
                    intermediateInputs.push_back(col);
                }
            }
            if (!intermediateInputs.empty()) {
                os << " | IN: ";
                for (size_t i = 0; i < intermediateInputs.size(); ++i) {
                    if (i > 0) os << ", ";
                    os << escapeLabel(columnFullName(*intermediateInputs[i]));
                }
            }

            // Add output columns
            auto outputs = getOutputColumns(*item);
            if (!outputs.empty()) {
                os << " | OUT: ";
                for (size_t i = 0; i < outputs.size(); ++i) {
                    if (i > 0) os << ", ";
                    os << escapeLabel(columnFullName(*outputs[i]));
                }
            }

            os << "}\"";

            // Style based on node type
            if (item->opData_case() == WorkItem::kResultData) {
                os << ", style=filled, fillcolor=\"#90EE90\"";  // Light green for root
            } else if (leaves_.count(id)) {
                os << ", style=filled, fillcolor=\"#ADD8E6\"";  // Light blue for leaves
            }

            os << "];\n";
        }

        os << "\n";

        // Define edges from base columns to operators
        os << "    // Edges from base columns\n";
        for (const auto& [colName, itemIds] : baseColumns_) {
            std::string nodeId = baseColumnNodeId(colName);
            for (uint32_t itemId : itemIds) {
                os << "    " << nodeId << " -> node_" << itemId;
                os << " [style=dashed, color=\"#B8860B\"];\n";  // Dark golden dashed line
            }
        }
        os << "\n";

        // Define edges between operators (dependencies)
        os << "    // Edges between operators\n";
        for (const auto& [id, item] : items_) {
            for (int i = 0; i < item->dependson_size(); ++i) {
                uint32_t depId = item->dependson(i);
                os << "    node_" << depId << " -> node_" << id;

                // Add edge label with column info if available
                std::string edgeLabel = getEdgeLabel(*item, depId);
                if (!edgeLabel.empty()) {
                    os << " [label=\"" << escapeLabel(edgeLabel) << "\"]";
                }

                os << ";\n";
            }
        }

        os << "}\n";
    }

    // Export DOT to a file
    bool exportDotToFile(const std::string& filename,
                         const std::string& graphName = "PlanDAG") const {
        std::ofstream file(filename);
        if (!file.is_open()) {
            return false;
        }
        exportDot(file, graphName);
        return true;
    }

    // Get DOT as string
    std::string toDotString(const std::string& graphName = "PlanDAG") const {
        std::ostringstream oss;
        exportDot(oss, graphName);
        return oss.str();
    }

   private:
    // Helper to generate valid DOT node ID for base columns
    static std::string baseColumnNodeId(const std::string& colName) {
        std::string result = "base_";
        for (char c : colName) {
            if (std::isalnum(static_cast<unsigned char>(c))) {
                result += c;
            } else {
                result += '_';
            }
        }
        return result;
    }

    // Helper to escape special characters in DOT labels
    static std::string escapeLabel(const std::string& str) {
        std::string result;
        result.reserve(str.size());
        for (char c : str) {
            switch (c) {
                case '"':
                    result += "\\\"";
                    break;
                case '\\':
                    result += "\\\\";
                    break;
                case '<':
                    result += "\\<";
                    break;
                case '>':
                    result += "\\>";
                    break;
                case '{':
                    result += "\\{";
                    break;
                case '}':
                    result += "\\}";
                    break;
                case '|':
                    result += "\\|";
                    break;
                default:
                    result += c;
                    break;
            }
        }
        return result;
    }

    // Helper to convert CompType to string
    static std::string compTypeToString(CompType ct) {
        switch (ct) {
            case COMP_LT:
                return "LT";
            case COMP_LE:
                return "LE";
            case COMP_EQ:
                return "EQ";
            case COMP_GE:
                return "GE";
            case COMP_GT:
                return "GT";
            case COMP_NE:
                return "NE";
            case COMP_BETWEEN:
                return "BETWEEN";
            case COMP_IN:
                return "IN";
            default:
                return "?";
        }
    }

    // Helper to convert AggFunc to string
    static std::string aggFuncToString(AggFunc af) {
        switch (af) {
            case AGG_COUNT:
                return "COUNT";
            case AGG_SUM:
                return "SUM";
            case AGG_MIN:
                return "MIN";
            case AGG_MAX:
                return "MAX";
            case AGG_AVG:
                return "AVG";
            default:
                return "?";
        }
    }

    // Helper to convert RelOp to string
    static std::string relOpToString(RelOp ro) {
        switch (ro) {
            case REL_UNION:
                return "UNION";
            case REL_INTERSECTION:
                return "INTERSECT";
            case REL_NEGATION:
                return "NEGATE";
            default:
                return "?";
        }
    }

    // Get edge label showing which columns flow between nodes
    std::string getEdgeLabel(const WorkItem& item, uint32_t depId) const {
        const WorkItem* depItem = getItem(depId);
        if (!depItem) return "";

        // Get outputs from dependency and inputs from current item
        auto depOutputs = getOutputColumns(*depItem);
        auto myInputs = getInputColumns(item);

        // Find matching columns
        std::vector<std::string> matches;
        for (const auto* input : myInputs) {
            if (isBaseColumn(*input)) continue;

            std::string inputBase = columnBaseName(*input);
            for (const auto* output : depOutputs) {
                std::string outputBase = columnBaseName(*output);
                if (inputBase == outputBase || columnFullName(*input) == columnFullName(*output)) {
                    // Extract just the column name without table
                    matches.push_back(input->colname());
                    break;
                }
            }
        }

        // Return abbreviated label
        if (matches.empty()) return "";
        if (matches.size() == 1) return matches[0];
        if (matches.size() <= 3) {
            std::string result;
            for (size_t i = 0; i < matches.size(); ++i) {
                if (i > 0) result += ", ";
                result += matches[i];
            }
            return result;
        }
        return std::to_string(matches.size()) + " cols";
    }

    bool canReachRoot(uint32_t id, std::unordered_set<uint32_t>& visited) const {
        if (id == *rootId_) return true;
        if (visited.count(id)) return false;
        visited.insert(id);

        if (children_.count(id)) {
            for (uint32_t child : children_.at(id)) {
                if (canReachRoot(child, visited)) {
                    return true;
                }
            }
        }
        return false;
    }

    bool isTransitiveDependency(uint32_t itemId, uint32_t potentialDep) const {
        std::unordered_set<uint32_t> visited;
        return isTransitiveDependencyDFS(itemId, potentialDep, visited);
    }

    bool isTransitiveDependencyDFS(uint32_t current, uint32_t target,
                                   std::unordered_set<uint32_t>& visited) const {
        if (current == target) return true;
        if (visited.count(current)) return false;
        visited.insert(current);

        if (parents_.count(current)) {
            for (uint32_t dep : parents_.at(current)) {
                if (isTransitiveDependencyDFS(dep, target, visited)) {
                    return true;
                }
            }
        }
        return false;
    }

    bool hasCycleDFS(uint32_t id, std::unordered_set<uint32_t>& visited,
                     std::unordered_set<uint32_t>& recursionStack,
                     std::vector<uint32_t>& cyclePath) const {
        if (recursionStack.count(id)) {
            cyclePath.push_back(id);
            return true;
        }
        if (visited.count(id)) {
            return false;
        }

        visited.insert(id);
        recursionStack.insert(id);

        if (parents_.count(id)) {
            for (uint32_t dep : parents_.at(id)) {
                if (hasCycleDFS(dep, visited, recursionStack, cyclePath)) {
                    cyclePath.push_back(id);
                    return true;
                }
            }
        }

        recursionStack.erase(id);
        return false;
    }
};

}  // namespace plan
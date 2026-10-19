#include "QueryVectorizer.hpp"
#include "WorkItem.pb.h"
#include <cmath>
#include <algorithm>
#include <limits>
#include <unordered_set>

namespace lb {
    std::array<float, QueryVectorizer::NUM_BASE_FEATURES> QueryVectorizer::extractSingleDagFeatures(
        const plan::PlanDAG &dag,
        const std::unordered_map<std::string, uint64_t> &schema
    ) {
        float input_rows = 0.0f;
        float num_hashjoin = 0.0f;
        float num_sort = 0.0f;
        float num_materialize = 0.0f;
        float num_aggregation = 0.0f;
        float num_set_ops = 0.0f;
        float num_filters = 0.0f;
        float num_map = 0.0f;
        float num_groupby = 0.0f;
        float max_depth = 0.0f;
        float simd_level = 1.0f;

        std::unordered_set<std::string> scanned_tables;
        for (const auto &[col_name, _]: dag.getBaseColumns()) {
            auto pos = col_name.find(".");
            std::string table_name = (pos != std::string::npos) ? col_name.substr(0, pos) : col_name;
            scanned_tables.insert(table_name);
        }

        for (const auto &table_name: scanned_tables) {
            auto it = schema.find(table_name);
            if (it != schema.end()) {
                input_rows += static_cast<float>(it->second);
            }
        }

        if (input_rows > 0.0f) {
            input_rows = std::log10(input_rows);
        }

        for (const auto &[id, item]: dag.getAllItems()) {
            if (!item->has_operatorid()) continue;

            switch (item->operatorid()) {
                case OP_FILTER: num_filters += 1.0f;
                    break;
                case OP_HASHJOIN: num_hashjoin += 1.0f;
                    break;
                case OP_SORT: num_sort += 1.0f;
                    break;
                case OP_MATERIALIZE: num_materialize += 1.0f;
                    break;
                case OP_AGGREGATE: num_aggregation += 1.0f;
                    break;
                case OP_SETOPERATION: num_set_ops += 1.0f;
                    break;
                case OP_MAP: num_map += 1.0f;
                    break;
                case OP_GROUPBY: num_groupby += 1.0f;
                    break;
                default: break;
            }
        }

        std::unordered_map<uint32_t, float> depths;
        auto topo_order = dag.topologicalSort();

        for (uint32_t id: topo_order) {
            float d = 1.0f;
            for (uint32_t parent_id: dag.getParents(id)) {
                d = std::max(d, depths[parent_id] + 1.0f);
            }
            depths[id] = d;
            max_depth = std::max(max_depth, d);
        }

        return {
            input_rows, num_hashjoin, num_sort, num_materialize,
            num_aggregation, num_set_ops, num_filters, num_map,
            num_groupby, max_depth, simd_level
        };
    }

    std::vector<float> QueryVectorizer::vectorizeGroup(
        const std::vector<const plan::PlanDAG *> &dags,
        const std::unordered_map<std::string, uint64_t> &schema
    ) {
        std::vector<float> result(GROUP_VECTOR_SIZE, 0.0f);
        if (dags.empty()) return result;

        const size_t n = dags.size();
        std::vector<std::array<float, NUM_BASE_FEATURES> > matrix;
        matrix.reserve(n);

        for (const auto *dag: dags) {
            matrix.push_back(extractSingleDagFeatures(*dag, schema));
        }

        std::array<float, NUM_BASE_FEATURES> f_sum{};
        std::array<float, NUM_BASE_FEATURES> f_mean{};
        std::array<float, NUM_BASE_FEATURES> f_max{};
        std::array<float, NUM_BASE_FEATURES> f_std{};

        f_max.fill(-std::numeric_limits<float>::infinity());

        for (const auto &row: matrix) {
            for (size_t j = 0; j < NUM_BASE_FEATURES; ++j) {
                f_sum[j] += row[j];
                if (row[j] > f_max[j]) {
                    f_max[j] = row[j];
                }
            }
        }

        for (size_t j = 0; j < NUM_BASE_FEATURES; ++j) {
            f_mean[j] = f_sum[j] / static_cast<float>(n);
        }

        for (const auto &row: matrix) {
            for (size_t j = 0; j < NUM_BASE_FEATURES; ++j) {
                float diff = row[j] - f_mean[j];
                f_std[j] += diff * diff;
            }
        }
        for (size_t j = 0; j < NUM_BASE_FEATURES; ++j) {
            f_std[j] = std::sqrt(f_std[j] / static_cast<float>(n));
        }

        size_t idx = 0;
        for (float v: f_sum) result[idx++] = v;
        for (float v: f_mean) result[idx++] = v;
        for (float v: f_max) result[idx++] = v;
        for (float v: f_std) result[idx++] = v;
        result[idx] = static_cast<float>(n); // group size

        return result;
    }
} // namespace lb

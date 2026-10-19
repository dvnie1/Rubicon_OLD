#pragma once

#include <array>
#include <vector>
#include <unordered_map>
#include <string>
#include "PlanDAG.hpp"

namespace lb {
    class QueryVectorizer {
    public:
        static constexpr size_t NUM_BASE_FEATURES = 11;
        static constexpr size_t GROUP_VECTOR_SIZE = NUM_BASE_FEATURES * 4 + 1;

        static std::vector<float> vectorizeGroup(
            const std::vector<const plan::PlanDAG *> &dags,
            const std::unordered_map<std::string, uint64_t> &schema = {}
        );

    private:
        static std::array<float, NUM_BASE_FEATURES> extractSingleDagFeatures(
            const plan::PlanDAG &dag,
            const std::unordered_map<std::string, uint64_t> &schema
        );
    };
}

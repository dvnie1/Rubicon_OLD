#pragma once

#include <vector>
#include "PlanDAG.hpp"

namespace lb {

    class QueryVectorizer {
    public:
        static std::vector<float> vectorizeGroup(const std::vector<plan::PlanDAG>& dags);
    };

}
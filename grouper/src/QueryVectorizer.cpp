#include "QueryVectorizer.hpp"
#include "WorkItem.pb.h"

namespace lb {

    std::vector<float> QueryVectorizer::vectorizeGroup(const std::vector<const plan::PlanDAG*>& dags) {
        // TODO: implement all of the features
        std::vector<float> features;
        features.push_back(static_cast<float>(dags.size()));

        size_t total_items = 0;
        size_t total_joins = 0;
        size_t total_filters = 0;

        for (const auto* dag : dags) {
            total_items += dag->size();
            for (const auto& [id, item] : dag->getAllItems()) {
                if (item->opData_case() == WorkItem::kJoinData) {
                    total_joins++;
                } else if (item->opData_case() == WorkItem::kFilterData) {
                    total_filters++;
                }
            }
        }

        features.push_back(static_cast<float>(total_items));
        features.push_back(static_cast<float>(total_joins));
        features.push_back(static_cast<float>(total_filters));
        return features;
    }

}
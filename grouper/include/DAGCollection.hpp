#ifndef DAG_COLLECTION_HPP
#define DAG_COLLECTION_HPP

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "PlanDAG.hpp"
#include "TCPServer.hpp"
#include "Analysis.hpp"
#include "TransferAnalysis.hpp"

#include "AdaptiveLoadBalancer.hpp"

class DagCollection {
   public:
    DagCollection(tuddbs::TCPServer& server, lb::AdaptiveLoadBalancer& loadBalancer, uint64_t threshold, float maxOverhead);
    ~DagCollection();

    DagCollection(const DagCollection&) = delete;
    DagCollection& operator=(const DagCollection&) = delete;
    DagCollection(DagCollection&&) = delete;
    DagCollection& operator=(DagCollection&&) = delete;

    void add(plan::PlanDAG&& dag);
    void seal();
    bool is_sealed() const;
    const std::vector<plan::PlanDAG>& dags() const;

   private:
    void run();
    void analyze();

    void sendPlansImmediately();

    QueryPlan& renameTableNames(QueryPlan& plan, const std::unordered_map<std::string, std::string>& replacement_map);

    std::vector<plan::PlanDAG> dags_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> sealed_{false};
    std::atomic<bool> shutdown_{false};
    
    tuddbs::TCPServer& server_;

    const uint64_t CHUNK_SIZE = 1024 * 1024 * 4;  // 4 MiB operator chunk
    const std::string SCALE_FACTOR = "sf100";
    
    uint64_t threshold_;
    analysis::SupersetAbsorptionConfig groupingConfig_{.maxMergeOverhead = 2.0f};
    transfer_analysis::CXLSystemConfig hwConfig_;  // hardware cost model
    std::thread worker_thread_;

    lb::AdaptiveLoadBalancer& load_balancer_;
};

#endif  // DAG_COLLECTION_HPP
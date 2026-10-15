#ifndef DAG_COLLECTION_MANAGER_HPP
#define DAG_COLLECTION_MANAGER_HPP

#include "DAGCollection.hpp"
#include "TCPServer.hpp"

#include <memory>
#include <vector>
#include <chrono>
#include <optional>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <functional>

#include "AdaptiveLoadBalancer.hpp"

class DagCollectionManager {
public:
    using Duration = std::chrono::steady_clock::duration;
    using CollectionFactory = std::function<std::unique_ptr<DagCollection>()>;

    // Constructor with configurable window duration (default: 1 second)
    explicit DagCollectionManager(tuddbs::TCPServer& server, lb::AdaptiveLoadBalancer& loadBalancer, uint64_t threshold = 10, Duration window_duration = std::chrono::seconds(1), float maxOverhead = 2.0f);

    // Constructor with custom collection factory (for derived DagCollection types)
    DagCollectionManager(tuddbs::TCPServer& server, uint64_t threshold, float maxOverhead, Duration window_duration, CollectionFactory factory);

    ~DagCollectionManager();

    // Non-copyable, non-movable
    // DagCollectionManager(const DagCollectionManager&) = delete;
    // DagCollectionManager& operator=(const DagCollectionManager&) = delete;
    // DagCollectionManager(DagCollectionManager&&) = delete;
    // DagCollectionManager& operator=(DagCollectionManager&&) = delete;

    // Add a DAG - routes to appropriate collection based on time window
    void add_dag(plan::PlanDAG&& dag);

    // Force close the current window (useful for testing or shutdown)
    void flush();

    // Get the number of completed (sealed) collections
    std::size_t completed_collection_count() const;

    // Change the window duration (takes effect on next window)
    void set_window_duration(Duration duration);

private:
    void close_current_window();
    void ensure_current_collection();
    void timer_thread_func();

    Duration window_duration_;
    CollectionFactory collection_factory_;

    std::unique_ptr<DagCollection> current_collection_;
    std::vector<std::unique_ptr<DagCollection>> completed_collections_;
    
    std::optional<std::chrono::steady_clock::time_point> window_start_;
    mutable std::mutex mutex_;
    
    // Timer thread for automatic window closing
    std::condition_variable timer_cv_;
    std::atomic<bool> shutdown_{false};
    
    tuddbs::TCPServer& server_;
    
    uint64_t threshold_;
    float maxOverhead_;
    std::thread timer_thread_;
};

#endif // DAG_COLLECTION_MANAGER_HPP

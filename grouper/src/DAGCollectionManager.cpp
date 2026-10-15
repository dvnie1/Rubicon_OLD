#include "DAGCollectionManager.hpp"

DagCollectionManager::DagCollectionManager(tuddbs::TCPServer &server, lb::AdaptiveLoadBalancer& loadBalancer, uint64_t threshold, Duration window_duration, float maxOverhead)
    : DagCollectionManager(server, threshold, maxOverhead, window_duration, [&server, &loadBalancer, threshold, maxOverhead] {
        return std::make_unique<DagCollection>(server, loadBalancer, threshold, maxOverhead);
    })
{
}

DagCollectionManager::DagCollectionManager(
    tuddbs::TCPServer& server, 
    uint64_t threshold,
    float maxOverhead,
    Duration window_duration,
    CollectionFactory factory)
    : window_duration_(window_duration)
    , collection_factory_(std::move(factory))
    , server_(server)
    , threshold_(threshold)
    , maxOverhead_(maxOverhead)
    , timer_thread_(&DagCollectionManager::timer_thread_func, this)
{
}

DagCollectionManager::~DagCollectionManager() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
        
        // Seal current collection if it exists
        if (current_collection_) {
            current_collection_->seal();
            completed_collections_.push_back(std::move(current_collection_));
        }
    }
    
    timer_cv_.notify_one();
    
    if (timer_thread_.joinable()) {
        timer_thread_.join();
    }
    
    // Destructor of completed_collections_ will join all analysis threads
}

void DagCollectionManager::add_dag(plan::PlanDAG&& dag) {
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // Ensure we have a collection to add to
        ensure_current_collection();

        // If this is the first DAG in the collection, start the window timer
        if (!window_start_.has_value()) {
            window_start_ = std::chrono::steady_clock::now();
        }

        current_collection_->add(std::move(dag));
    }
    
    // Notify timer thread that a window may have started
    timer_cv_.notify_one();
}

void DagCollectionManager::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (current_collection_) {
        close_current_window();
    }
}

std::size_t DagCollectionManager::completed_collection_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return completed_collections_.size();
}

void DagCollectionManager::set_window_duration(Duration duration) {
    std::lock_guard<std::mutex> lock(mutex_);
    window_duration_ = duration;
}

void DagCollectionManager::close_current_window() {
    // Note: Caller must hold mutex_
    if (current_collection_) {
        current_collection_->seal();
        completed_collections_.push_back(std::move(current_collection_));
        current_collection_ = nullptr;
        window_start_ = std::nullopt;
    }
}

void DagCollectionManager::ensure_current_collection() {
    // Note: Caller must hold mutex_
    if (!current_collection_) {
        current_collection_ = collection_factory_();
    }
}

void DagCollectionManager::timer_thread_func() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        if (shutdown_) {
            return;
        }
        
        if (!window_start_.has_value()) {
            // No active window - wait until notified (new DAG arrives or shutdown)
            timer_cv_.wait(lock, [this] {
                return shutdown_.load() || window_start_.has_value();
            });
        } else {
            // Active window - calculate remaining time
            auto now = std::chrono::steady_clock::now();
            auto elapsed = now - window_start_.value();
            
            if (elapsed >= window_duration_) {
                // Window expired - close it
                close_current_window();
            } else {
                // Wait for remaining duration or until notified
                auto remaining = window_duration_ - elapsed;
                timer_cv_.wait_for(lock, remaining, [this] {
                    return shutdown_.load();
                });
                
                // After waking, check again if window should be closed
                if (!shutdown_ && window_start_.has_value()) {
                    now = std::chrono::steady_clock::now();
                    elapsed = now - window_start_.value();
                    if (elapsed >= window_duration_) {
                        close_current_window();
                    }
                }
            }
        }
    }
}
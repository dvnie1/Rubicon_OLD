#pragma once

#include <WorkRequest.pb.h>
#include <WorkResponse.pb.h>

#include <../../../tcp_stuff/include/TCPClient.hpp>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <operators/AbstractOperator.hpp>
#include <thread>
#include <vector>

/**
 * @brief An abstraction for a task that has to be performed by a UnitWorker.
 * TODO: Assess if this struct should encapsulate an individual onFinish callback instead of the WorkerPool.
 *
 */
struct Task {
    Task(std::unique_ptr<AbstractOperator> _op, uint64_t rt) : op(std::move(_op)), response_target(rt) {
    }
    ~Task() {
    }
    Task(const Task& other) = delete;  // Copy Ctor
    Task(Task&& other) : op(std::move(other.op)), original_work_item(other.original_work_item), response_target(other.response_target) {
    }
    Task& operator=(const Task& other) = delete;  // Copy Assignment
    Task& operator=(Task&& other) {
        std::swap(op, other.op);
        std::swap(original_work_item, other.original_work_item);
        std::swap(response_target, other.response_target);
        return *this;
    }

    // Task() = default;
    // ~Task() = default;
    // Task(const Task& other) = default;             // Copy Ctor
    // Task(Task&& other) = default;                  // Move Ctor
    // Task& operator=(const Task& other) = default;  // Copy Assignment
    // Task& operator=(Task&& other) = default;       // Move Assignment Ctor

    std::unique_ptr<AbstractOperator> op;
    WorkItem original_work_item;
    WorkResponse response;
    uint64_t response_target;
};

class UnitWorker;

typedef std::function<void()> EventLoop;
typedef std::function<void(const Task&)> TaskHandlerCallback;

class WorkerPool {
    friend UnitWorker;

   public:
    /**
     * @brief Construct a new Thread Pool object and start directly spawn some worker threads.
     *
     * @param num_threads How many threads to spawn on startup
     */
    explicit WorkerPool(const size_t num_threads = 0, const ssize_t node_affinity = 0);

    ~WorkerPool();

    /**
     * @brief Start a given amount of worker threads.
     *
     * @param num_threads The amount of threads that are spawned.
     */
    void start_workers(const size_t num_threads);

    /**
     * @brief Stops All (num_threads == 0) or num_threads threads of the pool. If num_threads is larger than the number of active threads, all workers will be halted.
     *
     * @param num_threads Amount of threads to be halted.
     */
    void stop_workers(const size_t num_threads = 0);

    /**
     * @brief Adjusts the active worker count to num_threads. Currently active tasks are finished. If all are workers are stopped and work is still in the queue, it will be handed back to the Message bus.
     *
     * @param num_threads Amount of threads to be halted.
     */
    void update_workers(const size_t num_threads);

    /**
     * @brief Set a functor, that should be called when an AbstractOperator has finished.
     *
     * @param finishFun The function callback after finishing an AbstractOperator.
     */
    void setOnFinish(TaskHandlerCallback finishFun);

    /**
     * @brief Set a functor, that should be called when an AbstractOperator needs to be forwarded/rerouted due to passiveness of this Unit.
     *
     * @param finishFun The function callback for forwarding an AbstractOperator.
     */
    void setOnForward(TaskHandlerCallback forwardFun);

    /**
     * @brief Dispatch a task object to onFinish, if set.
     *
     * @param t A task, which is properly initialized with a WorkResponse and response_destination.
     */
    void finalizeTask(Task& t);

    /**
     * @brief Add a new operator to the global worker queue.
     *
     * @param op Any operator that can be wrapper in the abstract interface.
     */
    void enqueue_operator(Task& task);

    /**
     * @brief Set the affinity of all current and future workers to a specific numa node.
     *
     * @param node The node on which workers should be scheduled on.
     */
    void set_affinity_to_node(const ssize_t node);

    int get_active_workers();

   private:
    std::atomic<size_t> active_workers;
    TaskHandlerCallback onFinish = nullptr;
    TaskHandlerCallback onForward = nullptr;
    std::map<size_t, cpu_set_t> node_affinity_map;
    std::vector<UnitWorker*> pool;
    std::vector<UnitWorker*> zombies;
    std::deque<Task> pending_ops;
    std::mutex zombie_mutex;
    std::mutex pool_mutex;
    std::mutex pop_mutex;
    std::condition_variable zombie_cv;
    std::condition_variable pool_cv;
    std::condition_variable pop_cv;
    std::thread vacuum;
    std::atomic<bool> stop_working{false};
    std::atomic<bool> stop_vacuum{false};

    cpu_set_t current_affinity;

    /**
     * @brief Removes a UnitWorker from the pool. This is meant to be called by the UnitWorker itself, as a friend class.
     *
     * @param worker The UnitWorker which unregisters itself after being told to stop.
     */
    void deregister_worker(UnitWorker* worker);

    /**
     * @brief A zombie is a worker, which was stopped by a nullptr Task.
     */
    void cleanup_zombies();
};

class UnitWorker {
   public:
    UnitWorker(WorkerPool* pool, cpu_set_t affinity);
    ~UnitWorker();

    void update_affinity(const cpu_set_t affinity);

    WorkerPool* owning_pool;
    std::atomic<bool> busy;
    std::thread _thread;
};

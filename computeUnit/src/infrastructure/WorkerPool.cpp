#include <numa.h>

#include <Logger.hpp>
#include <infrastructure/WorkerPool.hpp>
#include <sstream>

WorkerPool::WorkerPool(const size_t workers, const ssize_t node_affinity) : active_workers{0} {
    const size_t nodes = numa_num_configured_nodes();
    const size_t cores = numa_num_configured_cpus();

    for (size_t node = 0; node < nodes; ++node) {
        cpu_set_t affinity;
        CPU_ZERO(&affinity);
        node_affinity_map[node] = affinity;
    }

    for (size_t i = 0; i < cores; ++i) {
        auto node = numa_node_of_cpu(i);
        CPU_SET(i, &node_affinity_map[node]);
    }

    set_affinity_to_node(node_affinity);

    std::stringstream nodestring;
    for (size_t i = 0; i < cores; ++i) {
        nodestring << (CPU_ISSET(i, &current_affinity) ? "1" : "0");
    }
    LOG_INFO("WorkerPool Affinity: " << nodestring.str() << std::endl;)

    start_workers(workers);
    vacuum = std::thread([this]() {
        while (!stop_vacuum) {
            {
                std::unique_lock<std::mutex> _lk(zombie_mutex);
                zombie_cv.wait(_lk, [this] { return stop_vacuum || !zombies.empty(); });
                if (stop_vacuum && zombies.empty()) {
                    break;
                }
            }
            cleanup_zombies();
        }
    });
}

WorkerPool::~WorkerPool() {
    LOG_INFO("[WorkerPool] Destructor Called." << std::endl;) {
        std::lock_guard<std::mutex> _lk(pop_mutex);
        stop_working.store(true);
    }
    // Stopp all remaining workers
    stop_workers(0);

    {
        std::unique_lock<std::mutex> _lk(pool_mutex);
        pool_cv.wait(_lk, [this] { return pool.empty(); });
    }

    {
        std::lock_guard<std::mutex> _lk(zombie_mutex);
        stop_vacuum = true;
        zombie_cv.notify_all();
    }
    vacuum.join();
    LOG_INFO("Pending: " << pending_ops.size() << " Worker: " << pool.size() << " Zombies: " << zombies.size() << std::endl;)
}

void WorkerPool::setOnFinish(TaskHandlerCallback finishFun) {
    onFinish = finishFun;
}

void WorkerPool::setOnForward(TaskHandlerCallback forwardFun) {
    onForward = forwardFun;
}

void WorkerPool::finalizeTask(Task& t) {
    if (onFinish) {
        onFinish(t);
    }
}

void WorkerPool::start_workers(const size_t workers) {
    {
        std::lock_guard<std::mutex> _lk(pool_mutex);
        for (size_t i = 0; i < workers; ++i) {
            UnitWorker* worker = new UnitWorker(this, current_affinity);
            pool.push_back(worker);
        }
    }
    active_workers += workers;
}

void WorkerPool::stop_workers(const size_t count) {
    const size_t stop_count = (count == 0) ? active_workers.load() : count;
    {
        std::lock_guard<std::mutex> _lk(pop_mutex);
        if (stop_count == active_workers.load()) {
            LOG_CONSOLE("Pending operators after stop: " << pending_ops.size() << std::endl;)
            const size_t forward_count = pending_ops.size();
            Task my_forward(nullptr, 0);
            for (size_t i = 0; i < forward_count; ++i) {
                if (onForward) {
                    my_forward = std::move(pending_ops.back());
                    onForward(my_forward);
                    pending_ops.pop_back();
                }
            }
        }
        Task t(nullptr, 0);
        for (size_t i = 0; i < stop_count; ++i) {
            pending_ops.emplace_front(std::forward<Task>(t));
        }
    }
    active_workers -= stop_count;
    pop_cv.notify_all();
}

void WorkerPool::update_workers(const size_t num_threads) {
    if (num_threads < active_workers.load()) {
        // If, for some reason, num_threads is set below zero, we just stop all workers.
        const size_t to_be_stopped = active_workers.load() - num_threads;
        LOG_DEBUG1("Current workers: " << active_workers.load() << " To be stopped: " << to_be_stopped << std::endl;)
        stop_workers(to_be_stopped);
    } else if (num_threads > active_workers.load()) {
        const size_t to_be_started = num_threads - active_workers.load();
        LOG_DEBUG1("Current workers: " << active_workers.load() << " To be started: " << to_be_started << std::endl;)
        start_workers(to_be_started);
    }
}

void WorkerPool::deregister_worker(UnitWorker* worker) {
    std::lock_guard<std::mutex> _lk(zombie_mutex);
    zombies.push_back(worker);
    zombie_cv.notify_all();
}

void WorkerPool::cleanup_zombies() {
    std::vector<UnitWorker*> to_delete;
    {
        std::lock_guard<std::mutex> _lk_p(pool_mutex);
        std::lock_guard<std::mutex> _lk_z(zombie_mutex);
        for (auto* z : zombies) {
            pool.erase(std::remove(pool.begin(), pool.end(), z), pool.end());
            to_delete.push_back(z);
        }
        zombies.clear();
    }
    for (auto* w : to_delete) {
        delete w;  // join() now happens without holding any locks
    }
    pool_cv.notify_all();
}

void WorkerPool::enqueue_operator(Task& task) {
    {
        std::lock_guard<std::mutex> _lk(pop_mutex);
        pending_ops.emplace_back(std::move(task));
        pop_cv.notify_one();
    }
}

void WorkerPool::set_affinity_to_node(const ssize_t node) {
    if (node >= numa_num_configured_nodes()) {
        throw std::runtime_error("[WorkerPool] Trying to assign my affinity to a non-existent node. Terminating.");
        exit(-1);
    }

    if (node >= 0) {
        current_affinity = node_affinity_map[node];
    } else {
        CPU_ZERO(&current_affinity);
        for (auto nodemask : node_affinity_map) {
            CPU_OR(&current_affinity, &current_affinity, &nodemask.second);
        }
    }
    std::lock_guard<std::mutex> _lk(pool_mutex);
    for (auto worker : pool) {
        worker->update_affinity(current_affinity);
    }
}


int WorkerPool::get_active_workers() {
    return active_workers.load();
}

UnitWorker::UnitWorker(WorkerPool* pool, cpu_set_t affinity) : owning_pool(pool) {
    EventLoop worker_loop = [this]() {
        while (true) {
            Task my_task(nullptr, 0);
            {
                std::unique_lock<std::mutex> lock(owning_pool->pop_mutex);
                owning_pool->pop_cv.wait(lock, [this] { return !owning_pool->pending_ops.empty() || owning_pool->stop_working; });

                // Should we really wait for completion of all pending jobs?
                if (owning_pool->stop_working && owning_pool->pending_ops.empty()) {
                    return;
                }
                busy = true;
                my_task = std::move(owning_pool->pending_ops.front());
                owning_pool->pending_ops.pop_front();
            }
            // A nullptr is pushed via stop_workers to terminate the first thread that finds it
            if (my_task.op == nullptr) {
                LOG_DEBUG1("[EventLoop] I (" << this << ") found a nullptr -- Terminating." << std::endl;)
                break;
            }
            my_task.response = std::move(my_task.op->run());
            owning_pool->onFinish(my_task);
            busy = false;
        }
        busy = false;
        LOG_DEBUG1("[Worker] Acquiring Zombie Mutex, let me die." << std::endl;)
        owning_pool->deregister_worker(this);
    };

    busy = false;
    _thread = std::thread([this, affinity, worker_loop]() {
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &affinity);
        if (rc != 0) {
            LOG_ERROR("[UnitWorker::UnitWorker] Error calling pthread_setaffinity_np: " << rc << std::endl;)
            exit(-10);
        }
        worker_loop();
    });
}

UnitWorker::~UnitWorker() {
    if (_thread.joinable()) {
        _thread.join();
    } else {
        LOG_WARNING("[UnitWorker] Thread is not joinable? Something is wrong" << std::endl;)
    }
}

void UnitWorker::update_affinity(const cpu_set_t affinity) {
    int rc = pthread_setaffinity_np(_thread.native_handle(), sizeof(cpu_set_t), &affinity);
    if (rc != 0) {
        LOG_ERROR("[UnitWorker] Error calling pthread_setaffinity_np: " << rc << std::endl;)
        exit(-10);
    }
}

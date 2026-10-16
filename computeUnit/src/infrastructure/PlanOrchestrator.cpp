#include <infrastructure/PlanOrchestrator.hpp>

#include <Logger.hpp>

namespace orchestrator {

PlanOrchestrator::PlanOrchestrator(Config config) {
    config_ = std::move(config);
}

PlanOrchestrator::~PlanOrchestrator() {
    shutdown();
}

void PlanOrchestrator::shutdown() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return;  // Already shut down
    }

    // Signal orchestrator thread
    {
        std::lock_guard<std::mutex> lock(eventQueueMutex_);
        eventQueue_.push(ShutdownEvent{});
    }
    eventQueueCV_.notify_one();

    // Signal GC thread
    cleanupCV_.notify_one();

    // Wait for threads to finish
    if (orchestratorThread_.joinable()) {
        orchestratorThread_.join();
    }
    if (gcThread_.joinable()) {
        gcThread_.join();
    }
}

void PlanOrchestrator::setDispatcher(OperatorDispatcher* dispatcher) {
    dispatcher_ = dispatcher;
    running_ = true;
    orchestratorThread_ = std::thread(&PlanOrchestrator::orchestratorThreadFunc, this);
    gcThread_ = std::thread(&PlanOrchestrator::gcThreadFunc, this);
}

bool PlanOrchestrator::submitPlan(const QueryPlan& plan, const uint64_t target_uuid) {
    if (!running_) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(eventQueueMutex_);
        eventQueue_.push(SubmitPlanEvent{plan, target_uuid});
    }
    eventQueueCV_.notify_one();
    return true;
}

void PlanOrchestrator::onItemCompleted(uint32_t planId, uint32_t itemId) {
    if (!running_) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(eventQueueMutex_);
        eventQueue_.push(ItemCompletedEvent{planId, itemId});
    }
    eventQueueCV_.notify_one();
}

void PlanOrchestrator::onItemFailed(uint32_t planId, uint32_t itemId) {
    if (!running_) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(eventQueueMutex_);
        eventQueue_.push(ItemFailedEvent{planId, itemId});
    }
    eventQueueCV_.notify_one();
}

void PlanOrchestrator::cancelPlan(uint32_t planId) {
    if (!running_) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(eventQueueMutex_);
        eventQueue_.push(CancelPlanEvent{planId});
    }
    eventQueueCV_.notify_one();
}

void PlanOrchestrator::finalizePlan(uint32_t planId) {
    if (!running_) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(eventQueueMutex_);
        eventQueue_.push(FinalizePlanEvent{planId});
    }
    eventQueueCV_.notify_one();
}

std::optional<PlanStatus> PlanOrchestrator::getPlanStatus(uint32_t planId) const {
    std::lock_guard<std::mutex> lock(plansMutex_);
    auto it = plans_.find(planId);
    if (it == plans_.end()) {
        return std::nullopt;
    }
    return it->second.status;
}

std::optional<ItemStatus> PlanOrchestrator::getItemStatus(uint32_t planId, uint32_t itemId) const {
    std::lock_guard<std::mutex> lock(plansMutex_);
    auto planIt = plans_.find(planId);
    if (planIt == plans_.end()) {
        return std::nullopt;
    }
    auto itemIt = planIt->second.items.find(itemId);
    if (itemIt == planIt->second.items.end()) {
        return std::nullopt;
    }
    return itemIt->second.status;
}

void PlanOrchestrator::orchestratorThreadFunc() {
    while (running_) {
        Event event;
        {
            std::unique_lock<std::mutex> lock(eventQueueMutex_);
            eventQueueCV_.wait(lock, [this] {
                return !eventQueue_.empty() || !running_;
            });

            if (!running_ && eventQueue_.empty()) {
                break;
            }

            if (eventQueue_.empty()) {
                continue;
            }

            event = std::move(eventQueue_.front());
            eventQueue_.pop();
        }

        // Process the event
        std::visit([this](const auto& e) { this->processEvent(e); }, std::as_const(event));
    }
}

void PlanOrchestrator::gcThreadFunc() {
    while (running_) {
        std::vector<uint32_t> plansToCleanup;

        {
            std::unique_lock<std::mutex> lock(cleanupMutex_);
            cleanupCV_.wait_for(lock, config_.gcInterval, [this] {
                return !running_ || cleanupQueue_.size() >= config_.maxPendingCleanup;
            });

            if (!running_ && cleanupQueue_.empty()) {
                break;
            }

            // Drain the cleanup queue
            while (!cleanupQueue_.empty()) {
                plansToCleanup.push_back(cleanupQueue_.front());
                cleanupQueue_.pop();
            }
        }

        // Perform actual cleanup
        if (!plansToCleanup.empty()) {
            std::lock_guard<std::mutex> lock(plansMutex_);
            for (uint32_t planId : plansToCleanup) {
                auto it = plans_.find(planId);
                if (it != plans_.end() && it->second.status == PlanStatus::PendingCleanup) {
                    plans_.erase(it);
                }
            }
        }
    }
}

void PlanOrchestrator::processEvent(const SubmitPlanEvent& event) {
    std::lock_guard<std::mutex> lock(plansMutex_);

    const auto& plan = event.plan;
    uint32_t planId = plan.planid();

    // Check if plan already exists
    if (plans_.find(planId) != plans_.end()) {
        // Plan already exists - could log warning or handle differently
        LOG_ERROR("[PlanOrchestrator::processEvent] Duplicate Plan ID" << std::endl;)
        return;
    }

    // Create new plan context
    PlanContext ctx;
    ctx.planId = planId;
    ctx.status = PlanStatus::Active;
    ctx.target_uuid = event.target_uuid;

    // Build dependency graph
    std::unordered_map<uint32_t, std::vector<uint32_t>> dependentsMap;

    for (int i = 0; i < plan.planitems_size(); ++i) {
        const auto& workItem = plan.planitems(i);
        uint32_t itemId = workItem.itemid();

        ItemState itemState;
        itemState.itemId = itemId;
        itemState.status = ItemStatus::Pending;
        itemState.workItem = workItem;

        // Copy dependencies
        for (int j = 0; j < workItem.dependson_size(); ++j) {
            uint32_t depId = workItem.dependson(j);
            itemState.dependencies.push_back(depId);
            dependentsMap[depId].push_back(itemId);
        }

        ctx.items[itemId] = std::move(itemState);
        ctx.pendingItems.insert(itemId);
    }

    // Set up dependents for each item
    for (auto& [itemId, itemState] : ctx.items) {
        auto it = dependentsMap.find(itemId);
        if (it != dependentsMap.end()) {
            itemState.dependents = std::move(it->second);
        }
    }

    // Store the context
    plans_[planId] = std::move(ctx);

    // Schedule initially ready items
    scheduleReadyItems(plans_[planId]);
}

void PlanOrchestrator::processEvent(const ItemCompletedEvent& event) {
    std::lock_guard<std::mutex> lock(plansMutex_);

    auto planIt = plans_.find(event.planId);
    if (planIt == plans_.end()) {
        LOG_ERROR("[PlanOrchestrator::processEvent] No plan with ID: " << event.planId << " was found." << std::endl;)
        return;
    }

    PlanContext& ctx = planIt->second;
    if (ctx.status != PlanStatus::Active) {
        return;
    }

    auto itemIt = ctx.items.find(event.itemId);
    if (itemIt == ctx.items.end()) {
        return;
    }

    ItemState& item = itemIt->second;
    if (item.status != ItemStatus::Dispatched) {
        return;
    }

    // Mark item as completed
    item.status = ItemStatus::Completed;
    ctx.pendingItems.erase(event.itemId);
    ctx.completedItems.insert(event.itemId);

    // Check if all items are complete
    if (ctx.pendingItems.empty()) {
        ctx.status = PlanStatus::Completed;
        return;
    }

    // Schedule newly ready items (dependents of the completed item)
    scheduleReadyItems(ctx);
}

void PlanOrchestrator::processEvent(const ItemFailedEvent& event) {
    std::lock_guard<std::mutex> lock(plansMutex_);

    auto planIt = plans_.find(event.planId);
    if (planIt == plans_.end()) {
        LOG_ERROR("[PlanOrchestrator::processEvent] No plan with ID: " << event.planId << " was found." << std::endl;)
        return;
    }

    PlanContext& ctx = planIt->second;
    if (ctx.status != PlanStatus::Active) {
        return;
    }

    auto itemIt = ctx.items.find(event.itemId);
    if (itemIt == ctx.items.end()) {
        return;
    }

    // Mark item as failed
    itemIt->second.status = ItemStatus::Failed;

    // Cancel the entire plan on failure
    ctx.status = PlanStatus::Cancelled;
    for (auto& [id, itemState] : ctx.items) {
        if (itemState.status == ItemStatus::Pending || itemState.status == ItemStatus::Ready) {
            itemState.status = ItemStatus::Cancelled;
        }
    }

    // Mark for cleanup
    markPlanForCleanup(event.planId);
}

void PlanOrchestrator::processEvent(const CancelPlanEvent& event) {
    std::lock_guard<std::mutex> lock(plansMutex_);

    auto planIt = plans_.find(event.planId);
    if (planIt == plans_.end()) {
        LOG_ERROR("[PlanOrchestrator::processEvent] No plan with ID: " << event.planId << " was found." << std::endl;)
        return;
    }

    PlanContext& ctx = planIt->second;
    if (ctx.status == PlanStatus::PendingCleanup) {
        return;
    }

    ctx.status = PlanStatus::Cancelled;

    // Cancel all pending/ready items
    for (auto& [id, itemState] : ctx.items) {
        if (itemState.status == ItemStatus::Pending || itemState.status == ItemStatus::Ready) {
            itemState.status = ItemStatus::Cancelled;
        }
    }

    markPlanForCleanup(event.planId);
}

void PlanOrchestrator::processEvent(const FinalizePlanEvent& event) {
    std::lock_guard<std::mutex> lock(plansMutex_);

    auto planIt = plans_.find(event.planId);
    if (planIt == plans_.end()) {
        LOG_ERROR("[PlanOrchestrator::processEvent] No plan with ID: " << event.planId << " was found." << std::endl;)
        return;
    }

    PlanContext& ctx = planIt->second;
    if (ctx.status == PlanStatus::PendingCleanup) {
        return;
    }

    // Can finalize if completed or cancelled
    if (ctx.status == PlanStatus::Completed || ctx.status == PlanStatus::Cancelled) {
        markPlanForCleanup(event.planId);
    }
}

void PlanOrchestrator::processEvent(const ShutdownEvent& event) {
    running_ = false;
}

void PlanOrchestrator::scheduleReadyItems(PlanContext& ctx) {
    if (ctx.status != PlanStatus::Active) {
        return;
    }

    for (uint32_t itemId : ctx.pendingItems) {
        auto it = ctx.items.find(itemId);
        if (it == ctx.items.end()) {
            continue;
        }

        ItemState& item = it->second;
        if (item.status != ItemStatus::Pending) {
            continue;
        }

        if (areAllDependenciesMet(ctx, item)) {
            item.status = ItemStatus::Ready;
            dispatchItem(ctx, item);
        }
    }
}

bool PlanOrchestrator::areAllDependenciesMet(const PlanContext& ctx, const ItemState& item) const {
    for (uint32_t depId : item.dependencies) {
        if (ctx.completedItems.find(depId) == ctx.completedItems.end()) {
            return false;
        }
    }
    return true;
}

void PlanOrchestrator::markPlanForCleanup(uint32_t planId) {
    // Update status (plansMutex_ should already be held)
    auto it = plans_.find(planId);
    if (it != plans_.end()) {
        it->second.status = PlanStatus::PendingCleanup;
    }

    // Add to cleanup queue
    {
        std::lock_guard<std::mutex> lock(cleanupMutex_);
        cleanupQueue_.push(planId);

        // Wake GC thread if queue is getting large
        if (cleanupQueue_.size() >= config_.maxPendingCleanup) {
            cleanupCV_.notify_one();
        }
    }
}

void PlanOrchestrator::dispatchItem(PlanContext& ctx, ItemState& item) {
    item.status = ItemStatus::Dispatched;
    dispatcher_->dispatch(ctx.target_uuid, item.workItem);
}

}  // namespace orchestrator
# RFC: TENT Local Transfer Admission Queue

## Summary

This RFC proposes a local transfer admission queue inside `TransferEngineImpl`. The queue orders logical transfers and applies backpressure before work is handed to `Transport::submitTransferTasks()` or `ProxyManager::submit()`.

This covers the local-runtime part of the Transfer Queue roadmap item in #1883, before adding distributed pressure signals or receiver credits.

It does not replace transport or ProxyManager execution. RDMA slice scheduling, CQ/QP handling, staging state machines, retries, and KVCache placement remain owned by their existing layers.

Phase 1 keeps the normal submit path direct and adds a private queue model plus compatibility tests. Distributed pressure and receiver credits can be added later as inputs to local admission; this RFC does not propose a cluster-wide per-transfer executor.

## Motivation

The current path is optimized for direct submission:

```text
submitTransfer()
  -> merge requests
  -> resolve transport or staging
  -> allocate SubBatch
  -> assign sub_task_id
  -> Transport::submitTransferTasks() or ProxyManager::submit()
```

This is fast when the process is not overloaded. Under heavy load, background prefetch or migration traffic and foreground Store `get` traffic can enter transport queues through the same direct path. By the time foreground work is visible to the runtime, transport-side queues may already contain background work.

Transport execution and ProxyManager staging remain owned by their existing layers. This RFC adds an earlier runtime admission point in `TransferEngineImpl::submitTransfer()`, so accepted work can be capped and latency-sensitive requests can be ordered before handoff.

## Current Code Shape

The queue would be inserted into the current `submitTransfer()` path:

```text
Current direct-submit shape

Application / Store
  |
  | allocateBatch(batch_size)
  v
TransferEngineImpl::submitTransfer(batch_id, requests)
  |
  | merge requests
  | append public TaskInfo entries to Batch::task_list
  | allocate SubBatch state and assign sub_task_id
  |
  +-- direct path --> Transport::submitTransferTasks()
  |                  -> RDMA / TCP / SHM / NVLink / GDS execution queues
  |
  +-- staged path --> ProxyManager::submit()
                     -> shard queue with TaskInfo*
                     -> stage buffers / internal batches / engine callbacks

Status and lifetime side paths

getTransferStatus()
  -> expects valid transport state for non-staging tasks
  -> treats UNSPEC as retry/fallback, not as "queued"

freeBatch()
  -> currently recycles through caller-thread-local state
  -> async queueing needs explicit engine-owned lifetime
```

In the direct path, public task state is published before any queue admission point. The queued path must therefore:

- Introduce explicit queue metadata instead of overloading `UNSPEC`, `TaskInfo::xport_priority`, or transport `sub_task_id`.
- Admission capacity is charged by public request/task ids. Merged or derived task ids resolve through an owner map instead of becoming independent queue items.
- Production async dispatch needs the engine to own batch lifetime and pin local buffers such as `Request::source`. `BatchID` is effectively a `Batch*`, and `SegmentTracker::ref_count` is not a transfer pin.
- Transport and ProxyManager keep execution ownership; the queue only controls admission and scheduling before handoff. Production staged queueing also needs stable task addresses because `ProxyManager::submit()` stores `TaskInfo*`.

## Architecture

The queue sits between caller submit and transport/proxy execution. L2 signals are inputs to the queue, not executors:

```text
                         future / optional signals
              +----------------------------------------------+
              | L2: distributed pressure                     |
              | host pressure, receiver credits, policy caps |
              +----------------------+-----------------------+
                                     |
                                     | advisory pressure only
                                     v
Application / Store
        |
        | submitTransfer(batch_id, requests)
        v
+------------------------------------------------------------------+
| TransferEngineImpl runtime boundary                              |
|                                                                  |
|  Local foundation:                                               |
|    validate -> charge -> owner map -> schedule                    |
|                                                                  |
|  Later production dispatch:                                      |
|    engine-owned batch refs / local-buffer pins / credits          |
|    transport/proxy handoff with dispatch windows                  |
+--------------------------+---------------------------------------+
                           |
                           | dispatch only after admission
                           v
+------------------------------------------------------------------+
| L0: execution owners                                             |
|                                                                  |
|  direct path: Transport::submitTransferTasks()                   |
|      -> RDMA / TCP / SHM / NVLink / GDS / io_uring queues        |
|                                                                  |
|  staged path: ProxyManager::submit()                             |
|      -> stage-buffer pipeline / internal batches / callbacks     |
+------------------------------------------------------------------+

```

In Phase 1, the normal submit path stays direct; the new code only exercises the queue model, side-table state, and owner mapping to ensure direct-path compatibility.

## Runtime Queue Design

### Data Model

A queue item represents one logical transfer owner. It is not an RDMA slice and does not copy payload data. The first implementation can keep this as a private model, but the model should already reflect production boundaries such as batch ownership, derived-task mapping, byte charge, priority class, route plan, queue state, and reserved internal work.

The logical owner is the single queued task for a merged request. Derived public task ids resolve to that owner and are not independently queued, dispatched, retried, or charged.

An implementation can start with a private shape like this:

```cpp
struct QueueOwner {
    QueueOwnerId id;
    BatchRef batch;                // model id first; engine-owned ref later
    size_t owner_task_id;
    std::vector<size_t> derived_task_ids;

    Request request;               // metadata only, no payload copy
    size_t task_charge;
    size_t byte_charge;
    PriorityClass priority;
    RoutePlan route;               // transport or staging decision

    QueueState state;
    uint64_t attempt_generation;
    TimePoint enqueue_time;
    CreditReservation credits;
    bool internal_staging_work;
};
```

The names and exact C++ layout can change. The important property here is that queue state is an explicit `QueueOwner::state` field.

### Admission

For the production queued path, queue admission for one `submitTransfer(batch_id, request_list)` call is all-or-nothing. If every logical owner derived from the call can be admitted, it returns `Status::OK`. If admission would exceed hard limits, it returns `Status::TooManyRequests` and leaves no partial submission behind. Transport execution can still fail later and report failure through status, matching the existing asynchronous contract.

When the production queued path is enabled, `Status::OK` means the work was published to the runtime queue, not necessarily that a transport has already accepted it. Applications may observe `PENDING` for tasks that have not reached `Transport::submitTransferTasks()` or `ProxyManager::submit()` yet.

This is the target production queued commit sequence:

```text
validate batch
check caller-provided batch request capacity
resolve request boundaries
merge requests
resolve initial transport or staging policy
compute owner tasks and queue charges
preflight per-transport sub-batch capacity
check queue limits
commit TaskInfo entries
enqueue owner items atomically
```

This avoids rolling back partially visible task ids and prevents submit hooks from covering rejected work.

Admission also rejects a submit call that would exceed the caller-provided batch request capacity or a planned transport sub-batch capacity. That rule belongs to the queued path, not to the currently disabled direct-submit path. Request merging can reduce the number of transport owners, but it cannot allow more public task ids than the batch was allocated to hold.

Queued work remains public `PENDING`. Internally, queued state is explicit: do not encode it as `UNSPEC`, a fake `sub_task_id`, or a missing `SubBatch`, because current status polling treats `UNSPEC` as retry/fallback.

Transport resolution, notification hooks, queue entries, owner mapping, and future local-buffer pins use the same publish-all-or-publish-none rule. If no route is available, the queued path either rejects before publishing or enters bounded `RoutePending`.

### State Model

The queue adds states before a task reaches transport ownership. For one logical owner, the intended transition shape is:

```text
              route refresh / policy retry
          +--------------------------------+
          |                                v
Queued -> RoutePending -> Queued -> Dispatching -> Submitted -> Completed
   |                             |              |          |
   |                             |              |          v
   |                             |              +-------> Failed
   |                             v
   +------------------------> Canceled
```

Public status mapping stays compatible: non-terminal queue states map to `PENDING`; terminal states map to the existing terminal status values.

Queue state lives in runtime queue metadata, not in `TaskInfo.status`. `TaskInfo.status` remains the public-facing task result seen by status polling and metrics.

| Internal state | Public status | Meaning |
| --- | --- | --- |
| `Queued` | `PENDING` | admitted, waiting for scheduler capacity |
| `RoutePending` | `PENDING` | waiting for a bounded route refresh or policy retry |
| `Dispatching` | `PENDING` | selected by dispatcher, not yet transport-owned |
| `Submitted` | `PENDING` | accepted by `Transport` or `ProxyManager` |
| `Completed` | `COMPLETED` | terminal success |
| `Failed` | `FAILED` | terminal failure |
| `Canceled` | internal | shutdown or future cancellation cleanup |

`RoutePending` is only for bounded route refresh or policy retry, such as temporarily stale metadata. It is not a generic overflow state. Once retry budget or deadline is exhausted, the owner task becomes terminal failure. `Canceled` is internal shutdown or future cancellation state; this RFC does not add public cancellation semantics.

Status lookup and notification hooks resolve derived task ids through their owner. Detailed hook firing and retirement semantics belong to the later lifecycle-correct dispatcher work.

### Dispatch Boundary

The production queue will sit at the transport/proxy dispatch boundary:

```text
runtime queue dispatcher
  -> direct path: Transport::submitTransferTasks()
  -> staged path: ProxyManager::submit()
```

The dispatcher groups ready items by transport to preserve batching, while honoring `batch->max_size` and per-transport sub-batch capacity. In the first patch, these rules can be validated with a synchronous model or mock dispatcher.

The transport handoff has several cases that should stay explicit:

| Case | Current code fact | Queued-path rule |
| --- | --- | --- |
| no transport slot yet | `getTransferStatus()` expects a real `sub_task_id` for non-staging tasks | return `PENDING` from queue metadata |
| append boundary | TENT assigns `sub_task_id` before `Transport::submitTransferTasks()`, and the transport API returns only `Status` | use append/commit or attempt-local state before async dispatch |
| `SubBatch` capacity | common transports return `TooManyRequests` when `request_list.size() + task_list.size() > max_size`; GDS has a larger backend charge model | preflight by transport charge; fail clearly if the owning batch is full |
| non-capacity submit failure | a transport may partially mutate its sub-batch before returning an invalid segment, SQE, batch submit, or other error | requeue only after non-mutating preflight, append/commit, attempt-local state, or proven rollback |

The first patch can model request merge, initial transport resolution, staging policy lookup, and transport charge as inputs. The normal `submitTransfer()` path remains direct-submit. In a later production queued path, final transport selection can move into the dispatcher when it can use queue depth, transport pressure, and receiver credits.

### Scheduling

The scheduler policy can be weighted by priority class and fair by bytes. Weighted deficit round robin with aging is a practical default: foreground traffic gets lower queueing latency, large transfers pay by byte charge, and old low-priority work still makes progress.

Because the current public API has no priority field, the first model can still validate multi-class scheduling. When a real queued submit path is enabled, caller traffic can initially use the default class until a later API or internal policy assigns foreground, background, prefetch, or staging-progress classes.

The runtime does not split large requests in the first slices. RDMA already slices after dispatch. Runtime-level chunking can be added later if benchmarks show priority inversion from very large requests.

The scheduler model selects entries whose byte charge fits the current class deficit and configured dispatch window. Production dispatch will also apply aggregate transport pressure and receiver credits when those signals exist. Merged requests are charged once, at the owner task; derived tasks observe the owner result and do not receive independent dispatch quota.

The first dispatcher can be only a model: enqueue owners, pick entries that fit the current task and byte window, and mark them as dispatching.

### Backpressure

Backpressure separates admission from dispatch. Admission limits decide whether a submit call can be accepted; dispatch limits decide whether accepted work may enter transport or ProxyManager ownership.

Admission can cap queued tasks, queued bytes, total outstanding work, and reserved internal credits. When a hard admission limit would be exceeded, `submitTransfer()` fails fast with `Status::TooManyRequests`. If a dispatch limit is saturated while queue capacity remains, the entry stays queued and public `PENDING`; this is different from rejecting a new submit.

Soft pressure comes from transports and affects dispatch rate, not admission correctness. No transport pressure API exists today, so the first reviewable PR does not depend on one. A later milestone can add aggregate signals such as saturated state, recommended dispatch bytes, backlog, retry deltas, and timeout deltas.

Runtime dispatch windows should use aggregate transport pressure only. Detailed RDMA state remains transport-owned and can later feed advisory pressure without being mutated by the queue.

The dispatcher also needs bounded dispatch windows even before rich transport pressure exists. If it drains accepted work into RDMA as fast as possible, priority only moves from the runtime queue into RDMA worker queues, and later latency-sensitive work is again ordered by transport-side queues.

Today, `TooManyRequests` covers several distinct conditions. The queued path therefore classifies it by source:

| Source | Queued-path handling |
| --- | --- |
| Queue admission | reject the whole submit before publishing tasks |
| `SubBatch` capacity | preflight reject; post-dispatch miss fails the owner or indicates a bug |
| Transport transient pressure | requeue with backoff and shrink the dispatch window |
| Proxy stage-buffer pressure | retry only on a typed retryable signal; otherwise fail explicitly |

Until transports expose typed pressure separately from capacity, a raw `Status::TooManyRequests` from `Transport::submitTransferTasks()` is better treated as a capacity or implementation failure at the runtime boundary, not as proof that the entry can be safely requeued.

### Staging

Staging stays owned by `ProxyManager`. The runtime queue decides when a logical staged transfer is admitted to ProxyManager, while stage-buffer allocation, the local/cross/remote chunk pipeline, and the remote delegate path stay inside ProxyManager. Today `ProxyManager::submit()` pushes into an internal shard `std::queue<StagingTask>` and returns `OK`; caller admission backpressure is handled before that handoff.

The queued path avoids introducing new long-lived raw `TaskInfo*` ownership. Production staged work should be addressable by stable batch ownership plus task id, or the batch storage needs to be made stable before ProxyManager receives the task.

Stage-buffer pressure appears after `ProxyManager::submit()`, inside worker-side pinning. `pinStageBuffer()` can report no free chunk as `Status::TooManyRequests`, while some staging cache failures are currently fatal. Production staged queue dispatch should turn these cases into bounded retry or explicit task failure, not process termination in a worker thread.

Proxy-generated sub-transfers allocate internal batches and call back into `TransferEngineImpl` today:

```text
user staged task
  -> ProxyManager worker
  -> internal allocateBatch() / submitTransfer()
  -> transport execution
```

Plain re-entry into the same user queue can deadlock already-admitted staged work:

```text
outer staged task admitted
  -> ProxyManager worker waits for internal transfer
     -> internal allocateBatch() / submitTransfer()
        -> same user queue is full or only user credits remain
           -> outer staged task cannot complete and release resources
```

Internal transfers therefore need one of two policies:

| Policy | Requirement |
| --- | --- |
| bypass runtime queue | separate accounting and bounded internal work |
| reserved internal class | non-blocking credits that user traffic cannot consume |

Post-accept ProxyManager stage-buffer pressure still needs a ProxyManager-visible retry/fail result or internal staging pressure signal.

ProxyManager shutdown is also part of the design. Tasks already accepted into shard queues need to be drained to terminal status or explicitly failed before workers exit; accepted-but-unstarted staging work should not disappear silently during runtime teardown.

The outer staged task remains complete only when `ProxyManager` reports the staging status as terminal.

## API, Config, And Metrics

| Area | Local foundation | Production direction |
| --- | --- | --- |
| public API | none; normal callers keep direct-submit behavior | later `SubmitOptions` for priority, deadline, or maximum queue wait |
| existing semantics | keep `allocateBatch(batch_size)`, task id ranges, notification hooks, polling, and Store `TransferFuture` cleanup unchanged | queue mode fixed before a batch accepts work |
| config | internal or test-only flag, default direct-submit | enablement, dispatcher count, queue/in-flight limits, default priority, reserved internal credits, aging |
| metrics | none required | queue depth, in-flight work, enqueue/dispatch/completion/reject counts, wait latency, transport latency, retry requeues, internal credit exhaustion |

## Implementation Plan

### Phase 1: Local Queue Foundation

Phase 1 proves local queue semantics while the queued submit path remains disabled. It does not export public headers, start a `TransferEngineImpl` background dispatcher, submit to real transports, or require Store and ProxyManager to compile against queue-specific types.

- add a private runtime admission and scheduler model with tests
- add queue-state metadata definitions without changing direct-submit behavior
- separate QoS priority class from `xport_priority`
- add a shared status transition helper for status, metrics, and hooks
- add tests for current direct-submit behavior to protect compatibility

Model-boundary pseudocode:

```cpp
Status TryAdmit(const SubmitShape& submit, QueueLimits& limits) {
    // Keep public task ids within the caller's allocated Batch capacity.
    if (submit.public_task_count > submit.batch_slots_left) {
        return Status::TooManyRequests("batch capacity");
    }
    if (!limits.CanReserve(submit.owner_count, submit.byte_charge)) {
        return Status::TooManyRequests("queue capacity");
    }

    limits.Reserve(submit.owner_count, submit.byte_charge);
    for (const auto& owner : submit.owners) {
        queue_.push_back(owner);

        // Queue and retry are owned by the merged owner task. Derived public
        // task ids only observe that owner's state.
        owner_map_[owner.owner_task_id] = owner.id;
        for (auto derived : owner.derived_task_ids) {
            derived_to_owner_[derived] = owner.id;
        }
    }
    return Status::OK();
}

std::vector<QueueOwnerId> PickReady(DispatchWindow& window) {
    std::vector<QueueOwnerId> ready;
    while (window.HasCapacity()) {
        auto owner = PickByPriorityAndDeficit(window.available_bytes);
        if (!owner.has_value()) break;

        MarkState(*owner, QueueState::Dispatching);
        window.Consume(owner->task_charge, owner->byte_charge);
        ready.push_back(owner->id);
    }
    return ready;
}

TransferStatusEnum PublicStatus(size_t public_task_id) const {
    auto owner = ResolveOwner(public_task_id);
    if (auto state = QueueStateOf(owner); state.has_value()) {
        // Queue-only states remain PENDING until a real transport/proxy owner
        // or a terminal queue result exists.
        return IsTerminal(*state) ? ToPublicStatus(*state) : PENDING;
    }
    return DirectSubmitStatus(public_task_id);
}
```

The exact names can change. The important contract is all-or-nothing admission, window-bounded dispatch, and public task ids resolving through one logical owner.

Phase 1 validates admission, scheduling, route-pending behavior, owner mapping, transport charge modeling, and direct-path compatibility while queued submit remains disabled.

The side table stays separate from `TaskInfo`: queued owners map to public `PENDING`, and derived task ids resolve through the owner map without copying transport fields into another `TaskInfo`. Any direct-path derived status cleanup should be a separate behavior-preserving refactor.

Phase 1 should stop before production async dispatch. Public API changes, transport pressure signals, ProxyManager ownership changes, and distributed credits can be discussed once the local model and compatibility tests are in place.

### Phase 2: Lifecycle-Correct Local Dispatch

Add explicit batch references or engine-wide batch ownership. `freeBatch()` becomes deferred cleanup. User polling and the completion tracker share the same transition helper.

It also adds transport handoff safety for partial sub-batch mutation, local-buffer pin enforcement, hook retirement, stale-completion rejection, and shutdown/drain ordering. One path owns terminal transition, credit release, and derived-task updates.

### Phase 3: Pressure And Credits

Add aggregate pressure sign俄als from transports and use them to adjust dispatch windows. Then add host-local pressure sharing and receiver credits. RDMA should expose aggregate pressure only; route selection and CQ/QP details remain inside RDMA. Distributed state influences dispatch windows and admission decisions; it does not become a per-transfer execution service.

Rollback is config-only: disable the transfer queue and use the current direct submit path. Performance gates should compare queue wait latency and transport execution latency separately; RDMA throughput regressions should be debugged without changing RDMA slicing, spraying, or CQ polling.

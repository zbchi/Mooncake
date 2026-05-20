# TENT Transfer Engine Contributor Architecture Guide

这份导读面向准备修改 `mooncake-transfer-engine/tent` 的贡献者。它的目标不是替代 API 文档，而是先把论文里的系统角色、文档里的抽象，以及当前 TENT 代码中的真实模块边界对齐起来。

## 一句话定位

Mooncake 论文讨论的是 KVCache-centric 的 LLM serving 架构，包含 prefill/decoding disaggregation、KVCache 调度、缓存池和数据搬运。TENT Transfer Engine 在这个体系里更接近底层 data plane: 它不做 KVCache 放置决策，也不做请求级 SLO 调度，而是负责把一组本地地址和远端 segment 地址之间的数据高效、异步、可重试地搬过去。

官方 Transfer Engine 文档把核心抽象归纳为 `Segment` 和 `BatchTransfer`。当前 TENT 代码里，这两个抽象主要落在：

- `Segment`: `SegmentDesc`、`BufferDesc`、`SegmentManager`、`SegmentTracker`
- `BatchTransfer`: `Batch`、`TaskInfo`、`Request`、`TransferStatus`
- data path 后端: `Transport` 及 RDMA、TCP、SHM、NVLink、GDS、io_uring 等实现
- runtime 策略: `TransferEngineImpl`

## 总架构图

```text
Application / Mooncake Store / vLLM connector
        |
        | C++ API: TransferEngine
        | C API:   tent_create_engine / tent_submit / tent_task_status
        v
+--------------------------------------------------------------+
| TransferEngineImpl                                           |
|                                                              |
|  API facade and runtime policy                               |
|  - init / teardown                                           |
|  - local segment lifecycle                                   |
|  - memory registration                                       |
|  - request merge and transport selection                     |
|  - staging fallback                                          |
|  - status polling, retry, notification, metrics              |
+--------------------------------------------------------------+
        |                  |                 |             |
        |                  |                 |             |
        v                  v                 v             v
+---------------+  +---------------+  +--------------+  +----------------+
| ControlService|  | SegmentManager|  | ProxyManager |  | transport_list |
| RPC server    |  | local/remote  |  | staging path |  | RDMA/TCP/SHM   |
| RPC client    |  | segment cache |  | workers      |  | NVLink/GDS/... |
+---------------+  +---------------+  +--------------+  +----------------+
        |                  |                 |             |
        v                  v                 v             v
+---------------+  +---------------+  +--------------+  +----------------+
| RPC peer      |  | SegmentRegistry| | stage buffers|  | Platform       |
| bootstrap     |  | p2p/central   |  | delegate RPC |  | CPU/CUDA/NPU   |
| delegate      |  | metadata      |  | transferSync |  | topology/probe |
+---------------+  +---------------+  +--------------+  +----------------+
```

读代码时可以把 `TransferEngineImpl` 当作主干。它不是一个具体传输后端，而是运行时编排器：它决定请求能不能合并、应该走哪个 transport、失败后换哪个 transport、什么时候改走 staging。

关键文件入口：

- `mooncake-transfer-engine/tent/include/tent/runtime/transfer_engine_impl.h:49`: `TaskInfo`
- `mooncake-transfer-engine/tent/include/tent/runtime/transfer_engine_impl.h:61`: `TransferEngineImpl`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:45`: `Batch`
- `mooncake-transfer-engine/tent/include/tent/common/types.h:40`: `Request`
- `mooncake-transfer-engine/tent/include/tent/runtime/transport.h:45`: `Transport`
- `mooncake-transfer-engine/tent/include/tent/runtime/segment.h:62`: `BufferDesc`
- `mooncake-transfer-engine/tent/include/tent/runtime/segment.h:168`: `SegmentDesc`

## 初始化路径

```text
TransferEngine / tent_create_engine
        |
        v
TransferEngineImpl::construct
        |
        +--> load config and environment overrides
        +--> discover / validate RPC listen address
        +--> Platform::getLoader + Topology::discover
        +--> create ControlService and start RPC server
        +--> setupLocalSegment
        +--> loadTransports
        +--> install each Transport
        +--> create ProxyManager
        +--> initialize metrics
```

代码位置：

- `mooncake-transfer-engine/tent/src/transfer_engine.cpp:23`: C++ facade 创建 `TransferEngineImpl`
- `mooncake-transfer-engine/tent/src/transfer_engine_c.cpp:41`: C API 创建 `TransferEngineImpl`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:258`: `construct()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:245`: `setupLocalSegment()`
- `mooncake-transfer-engine/tent/src/runtime/transport_loader.cpp:43`: `loadTransports()`
- `mooncake-transfer-engine/tent/src/runtime/control_plane.cpp:162`: `ControlService` 注册 RPC handlers

初始化完成后，本进程会拥有一个本地 memory segment。这个 segment 暂时可能没有用户 buffer，直到调用 `registerLocalMemory()` 后才会把具体地址区间发布出去。

## Segment 和元数据

```text
local process
  SegmentDesc
    name
    machine_id
    MemorySegmentDesc
      topology
      rpc_server_addr
      buffers[]
        addr, length, location
        transports[]
        transport_attrs
        ref_count
        internal

metadata registry or p2p RPC
  stores / returns SegmentDesc

remote process
  openSegment(name) -> SegmentID
  getRemoteCached(SegmentID) -> SegmentDesc*
```

核心分工：

- `SegmentDesc` 是可被传输访问的地址空间描述。
- `BufferDesc` 是一个已经注册的连续内存区间，transport 会把自己的 rkey、IPC handle、shm path 等写入 `transport_attrs` 或兼容字段。
- `SegmentManager` 管 local/remote segment 名字和 `SegmentID` 的映射，并维护 thread-local remote cache。
- `SegmentTracker` 管本地 buffer 的插入、引用计数、删除和排序。

代码位置：

- `mooncake-transfer-engine/tent/src/runtime/segment_manager.cpp:35`: `openRemote()`
- `mooncake-transfer-engine/tent/src/runtime/segment_manager.cpp:61`: `getRemoteCached()`
- `mooncake-transfer-engine/tent/src/runtime/segment_tracker.cpp:87`: `addInBatch()`
- `mooncake-transfer-engine/tent/src/runtime/segment_tracker.cpp:126`: `remove()`
- `mooncake-transfer-engine/tent/src/runtime/control_plane.cpp:220`: `onGetSegmentDesc()`

贡献时要特别注意：远端可见性依赖 `metadata_->segmentManager().synchronizeLocal()`。注册或注销本地内存时，如果忘了同步，远端会继续使用旧的 `SegmentDesc` 缓存，表现通常是 transport 选择失败或 stale key。

## 内存注册生命周期

```text
user buffer
   |
   v
registerLocalMemory(addr, size, options)
   |
   +--> select candidate transports
   +--> transport warmupMemory, if available
   +--> Platform::getLoader().getLocation
   +--> build BufferDesc(addr, length, location, regions)
   +--> SegmentTracker::addInBatch
          |
          +--> for each candidate transport:
                  transport->addMemoryBuffer(descs, options)
   |
   +--> SegmentManager::synchronizeLocal
```

反向注销路径：

```text
unregisterLocalMemory
   |
   +--> SegmentTracker::remove
          |
          +--> transport->removeMemoryBuffer(desc)
   |
   +--> SegmentManager::synchronizeLocal
```

代码位置：

- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:605`: advanced `registerLocalMemory()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:666`: single-buffer `unregisterLocalMemory()`
- `mooncake-transfer-engine/tent/include/tent/runtime/transport.h:80`: `allocateLocalMemory()`
- `mooncake-transfer-engine/tent/include/tent/runtime/transport.h:91`: `warmupMemory()`
- `mooncake-transfer-engine/tent/include/tent/runtime/transport.h:93`: `addMemoryBuffer()`
- `mooncake-transfer-engine/tent/include/tent/runtime/transport.h:107`: `removeMemoryBuffer()`

这里有两个容易踩的点：

- `allocateLocalMemory()` 只是分配并记录由哪个 transport 分配，不等于远端可见。远端可见需要 `registerLocalMemory()`。
- `internal=true` 的 buffer 不会出现在 `getSegmentInfo()` 结果里，但仍可作为 stage buffer 被 runtime 自己使用。

## Submit 数据路径

```text
submitTransfer(batch_id, request_list)
        |
        v
append TaskInfo placeholders
        |
        v
resolveRequestBoundaries
        |
        v
mergeRequests
        |
        v
for each merged request:
        |
        +--> resolveTransport(request, priority=0)
        |       |
        |       +--> get local memory type
        |       +--> get target SegmentDesc
        |       +--> find target BufferDesc
        |       +--> check same-machine rule for SHM/NVLink
        |       +--> check transport capabilities
        |
        +--> if TCP and staging policy exists:
        |       ProxyManager::submit
        |
        +--> else:
                allocate transport SubBatch if needed
                push request into classified_request_list[type]

for each transport type:
        transport->submitTransferTasks(sub_batch, requests)
```

代码位置：

- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:1050`: `submitTransfer()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:860`: `mergeRequests()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:1039`: `resolveTransport()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:774`: `getTransportType()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:983`: `findStagingPolicy()`

`mergeRequests()` 只合并同 opcode、同 target segment、同 source buffer、同 target buffer 且地址连续的请求。被合并掉的原始 task 会标记 `TaskInfo::derived=true`，状态聚合和 metrics 会跳过 derived task，避免重复计算。

## Transport 选择规则

```text
Request
  source: local pointer
  target_id + target_offset: remote or local target
        |
        v
getTransportType
        |
        +--> local memory type from Platform
        +--> target segment type
              |
              +--> File segment:
              |       prefer GDS, then IOURING
              |
              +--> Memory segment:
                      find target BufferDesc
                      same machine?
                      iterate BufferDesc.transports
                      filter by capability matrix
                      return nth candidate by priority
```

Capability matrix 来自每个 transport 的 `Transport::capabilities()`：

- CPU to CPU: `dram_to_dram`
- CPU to GPU: `dram_to_gpu`
- GPU to CPU: `gpu_to_dram`
- GPU to GPU: `gpu_to_gpu`
- CPU or GPU to file: `dram_to_file` / `gpu_to_file`

失败重试的关键是 `priority`。第一次选择 priority 0，失败后 `resubmitTransferTask()` 会增加 `task.xport_priority`，再调一次 `resolveTransport()`，从同一候选集合里选下一个 transport。

代码位置：

- `mooncake-transfer-engine/tent/include/tent/runtime/transport.h:36`: `Capabilities`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:745`: memory/file capability check helpers
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:1201`: `resubmitTransferTask()`

当前加载顺序在 `loadTransports()`，但实际请求选择顺序主要由远端 `BufferDesc::transports` 决定。这个列表是在本地注册内存时由各 transport 的 `addMemoryBuffer()` 填充出来的。

## Staging fallback

当直接路径不可用时，TENT 可以借助 CPU/GPU stage buffer 把一次传输拆成多段。典型例子是 RDMA 不支持 GPU direct 时，GPU buffer 需要先拷到近端 CPU 内存，再通过 RDMA/TCP 传到对端，必要时对端再从 CPU stage buffer 拷到 GPU。

```text
submitTransfer
   |
   +--> selected transport is TCP
          |
          +--> findStagingPolicy
                 returns [remote_rpc_server_addr,
                          local_stage_location,
                          remote_stage_location]
          |
          +--> ProxyManager::submit(TaskInfo)

ProxyManager worker
   |
   +--> pin local/remote stage buffers
   +--> split request into chunks
   +--> per chunk state machine:
          PRE -> CROSS -> POST -> FINISH
   +--> use TransferEngineImpl::submitTransfer for local/cross stages
   +--> use ControlClient::delegate for remote local-copy stage
```

WRITE 的逻辑顺序：

```text
source -> local stage -> cross-node transfer -> remote stage -> target
```

READ 的逻辑顺序：

```text
target -> remote stage -> cross-node transfer -> local stage -> source
```

代码位置：

- `mooncake-transfer-engine/tent/include/tent/runtime/proxy_manager.h:37`: `ProxyManager`
- `mooncake-transfer-engine/tent/src/runtime/proxy_manager.cpp:143`: `submit()`
- `mooncake-transfer-engine/tent/src/runtime/proxy_manager.cpp:235`: `transferEventLoop()`
- `mooncake-transfer-engine/tent/src/runtime/proxy_manager.cpp:544`: `pinStageBuffer()`
- `mooncake-transfer-engine/tent/src/runtime/control_plane.cpp:118`: `ControlClient::delegate()`
- `mooncake-transfer-engine/tent/src/runtime/control_plane.cpp:288`: `ControlService::onDelegate()`

贡献 staging 相关代码时，要把 `TaskInfo::staging_status` 当成跨 worker 线程状态看待。`getTransferStatus()` 不会主动执行 staging 状态机，它只是读取 `ProxyManager` 中 worker 推进后的状态。

## 状态查询、重试和释放

```text
getTransferStatus(batch, task_id)
        |
        +--> if staging:
        |       staging_proxy_->getStatus
        |
        +--> else if task.type == UNSPEC:
        |       resubmitTransferTask
        |
        +--> else:
                transport->getTransferStatus(sub_batch, sub_task_id)

        |
        +--> if FAILED:
        |       try resubmitTransferTask with next priority
        |
        +--> update TaskInfo.status
        +--> record terminal metrics once
        +--> maybe fire submit notification hook
```

代码位置：

- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:1241`: per-task `getTransferStatus()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:1295`: whole-batch `getTransferStatus()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:1373`: `transferSync()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:1402`: `recordTaskCompletionMetrics()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:708`: `allocateBatch()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:716`: `freeBatch()`
- `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp:724`: `lazyFreeBatch()`

注意：`BatchID` 在实现里就是 `Batch*` cast 成整数。`freeBatch()` 不是立即释放所有资源，而是放进 thread-local freelist，再由 `lazyFreeBatch()` 在确认 batch 不再 pending 后释放各 transport 的 sub-batch。

## Control plane 和 RPC

```text
ControlService
  GetSegmentDesc  -> return local SegmentDesc JSON
  BootstrapRdma   -> transport RDMA bootstrap callback
  SendData        -> RPC fallback write into registered local buffer
  RecvData        -> RPC fallback read from registered local buffer
  Notify          -> notification callback
  Delegate        -> remote impl_->transferSync()
  Pin/Unpin       -> remote stage buffer management
```

这层不是集中的调度器。它做两件事：

- segment metadata 的读写和缓存失效
- transport bootstrap、delegate、stage buffer pin/unpin 等点对点协作

代码位置：

- `mooncake-transfer-engine/tent/include/tent/runtime/control_plane.h:59`: `ControlClient`
- `mooncake-transfer-engine/tent/include/tent/runtime/control_plane.h:102`: `ControlService`
- `mooncake-transfer-engine/tent/src/runtime/control_plane.cpp:151`: registry 类型选择
- `mooncake-transfer-engine/tent/src/runtime/control_plane.cpp:220`: `onGetSegmentDesc()`
- `mooncake-transfer-engine/tent/src/runtime/control_plane.cpp:288`: `onDelegate()`

## 贡献入口速查

### 想加一个 transport

优先读：

- `mooncake-transfer-engine/tent/include/tent/runtime/transport.h:45`
- `mooncake-transfer-engine/tent/src/runtime/transport_loader.cpp:43`
- 一个现有简单实现，比如 `src/transport/tcp/tcp_transport.cpp` 或 `src/transport/shm/shm_transport.cpp`

通常要改：

- `TransportType` enum: `mooncake-transfer-engine/tent/include/tent/common/types.h:73`
- `kSupportedTransportTypes`: `mooncake-transfer-engine/tent/include/tent/common/types.h:83`
- `loadTransports()`
- 新 transport 的 `Capabilities`
- `addMemoryBuffer()` / `removeMemoryBuffer()` 写入和清理 `BufferDesc`
- `allocateSubBatch()` / `submitTransferTasks()` / `getTransferStatus()`
- 如果需要 C API 暴露，还要看 `tent_memory_options_t` 和转换逻辑

### 想改 transport 选择策略

优先读：

- `getTransportType()`
- `resolveTransport()`
- `resubmitTransferTask()`
- `findStagingPolicy()`

这里最重要的约束是：选择策略依赖本地 `Platform` 探测结果，也依赖远端 `BufferDesc` 中 transport 注册信息。不要只改本地优先级，否则可能选到远端没有注册过的路径。

### 想改 request 合并或 batch 行为

优先读：

- `Batch`
- `TaskInfo`
- `mergeRequests()`
- `getTransferStatus()` 的 derived task 处理
- `tests/request_merge_test.cpp`

风险点是状态和 metrics 去重。合并后的 request 只提交一次，但用户仍然按原始 request 数量查询状态。

### 想改 metadata 或 segment 生命周期

优先读：

- `SegmentDesc` / `BufferDesc`
- `SegmentManager`
- `SegmentTracker`
- `ControlService::onGetSegmentDesc`

风险点是兼容 JSON 格式、thread-local remote cache、以及注册/注销后的 `synchronizeLocal()`。

### 想改 staging

优先读：

- `findStagingPolicy()`
- `ProxyManager::transferEventLoop()`
- `ControlClient::delegate()`
- `ControlService::onDelegate()`

风险点是 stage buffer pin/unpin、chunk 状态机、远端 delegate 的同步语义，以及 worker 线程和用户 polling 线程之间的状态发布。

### 想改 metrics 或 notification

优先读：

- `maybeFireSubmitHooks()`
- `recordTaskCompletionMetrics()`
- `sendNotification()` / `receiveNotification()`
- 对应 transport 的 notification 支持

metrics 只应在 task 从 `PENDING` 进入终态时记录一次，derived task 不能重复记录。

## 推荐读代码顺序

1. `include/tent/common/types.h`: 先记住 `Request`、`SegmentID`、`BatchID`、`TransportType`。
2. `include/tent/runtime/transport.h`: 理解所有 transport 必须实现的接口。
3. `include/tent/runtime/segment.h`: 理解远端实际拿到的 metadata 长什么样。
4. `src/runtime/transfer_engine_impl.cpp:258`: 看初始化。
5. `src/runtime/transfer_engine_impl.cpp:605`: 看内存如何注册成 Buffer。
6. `src/runtime/transfer_engine_impl.cpp:1050`: 看一次 submit 如何变成 transport sub-batch。
7. `src/runtime/transfer_engine_impl.cpp:1241`: 看 polling 如何推进重试和完成。
8. `src/runtime/proxy_manager.cpp:235`: 最后看 staging，这部分依赖前面的 batch 和 RPC 语义。

## 概念对照表

| 论文/文档概念 | TENT 代码落点 | 说明 |
| --- | --- | --- |
| KVCache movement | `Request` + `submitTransfer()` | TENT 不理解 KVCache，只搬字节区间。 |
| Messenger / data transfer | `TransferEngineImpl` + `Transport` | 负责点对点数据路径。 |
| Segment | `SegmentDesc` + `SegmentManager` | 远端可寻址空间。 |
| Buffer | `BufferDesc` + `SegmentTracker` | 已注册的连续地址范围。 |
| BatchTransfer | `Batch` + `TaskInfo` | 用户提交的一组异步请求。 |
| Topology-aware path selection | `Topology` + transport attrs + `getTransportType()` | 当前策略入口在 runtime，具体路径细节在 transport。 |
| GPUDirect/RDMA/NVLink 等后端 | `Transport` subclasses | 统一挂到 `transport_list_`。 |
| staging buffer | `ProxyManager` | direct path 不满足能力矩阵时的中转路径。 |
| failure fallback | `getTransferStatus()` + `resubmitTransferTask()` | polling 过程中触发下一优先级 transport。 |

## 外部背景资料

- Mooncake paper: <https://arxiv.org/abs/2407.00079>
- Transfer Engine design doc: <https://kvcache-ai.github.io/Mooncake/design/transfer-engine/index.html>


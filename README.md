# 本地集群作业调度器（Local HPC Job Scheduler）

一个纯本地运行、零外部依赖的集群作业调度能力演示：用进程内数据结构模拟**节点**与**工作进程**，
不接真实集群、外部服务或真实凭据。覆盖作业从提交、排队、整组调度、运行、心跳、失败重试、
取消/完成竞争到进程重启恢复的完整生命周期，并在资源竞争、节点掉线、重复上报与并发操作下
给出**一致且可解释**的结果。

技术栈沿用工程骨架：Java 21、Spring Boot 3.4（Web）、Jackson、JUnit 5，Maven 构建。

---

## 1. 快速开始

```bash
# 构建 + 全部单元/并发/Web 测试
mvn test

# 打包（跳过测试）
mvn package -DskipTests

# 以演示集群启动（自动注册 3 个模拟节点）
mvn spring-boot:run \
  -Dspring-boot.run.arguments="--scheduler.demo-cluster=true \
  --scheduler.node-timeout-ms=3600000 --scheduler.heartbeat-timeout-ms=3600000"
```

> 演示时把超时调大，避免你还没操作节点就被后台巡检判为失联。
> 应用默认使用随机端口（`server.port=0`），启动日志中的 `Tomcat started on port ...` 即为实际端口。
> 也可用 `--server.port=8080` 固定。

启动后即可用 HTTP 操作（无鉴权，纯本地）：

| 方法 & 路径 | 说明 |
|---|---|
| `POST /api/nodes` | 注册/重复注册节点（幂等） |
| `POST /api/nodes/{id}/heartbeat` | 节点心跳（保活） |
| `POST /api/nodes/{id}/down?lost=true/false` | 心跳失联 / 主动下线 |
| `POST /api/nodes/{id}/recover` | 节点恢复在线并触发调度 |
| `GET  /api/nodes` | 节点及实时用量 |
| `POST /api/jobs` | 提交单作业（arraySize=1）或作业阵列 |
| `GET  /api/jobs` / `GET /api/jobs/{id}` | 作业列表 / 详情（含排队原因） |
| `POST /api/jobs/{id}/cancel` | 取消作业 |
| `POST /api/tasks/{jobId}.{index}/heartbeat` | 工作进程心跳 |
| `POST /api/tasks/{jobId}.{index}/complete` | 工作进程完成上报（重复上报幂等） |
| `POST /api/schedule` | 手动触发一轮整组调度（测试用） |
| `POST /api/detect-timeouts` | 手动触发超时/失联巡检 |
| `GET  /api/snapshot` | 集群与作业总览 |

任务 ID 规则为 `作业ID.阵列下标`，例如 `arr1.0`、`arr1.1`（点号便于直接放在 URL 路径中）。

### 1.1 一次完整的手工验证

```bash
B=http://localhost:8080

# 节点：3 台，8/8/4 核
curl -s -X POST $B/api/nodes -H 'Content-Type: application/json' \
  -d '{"nodeId":"n1","cpus":8,"memoryMb":8192,"gpus":2}'
curl -s -X POST $B/api/nodes -H 'Content-Type: application/json' \
  -d '{"nodeId":"n2","cpus":8,"memoryMb":8192,"gpus":2}'
curl -s -X POST $B/api/nodes -H 'Content-Type: application/json' \
  -d '{"nodeId":"n3","cpus":4,"memoryMb":4096,"gpus":1}'

# 阵列 4 任务 × 4 核 1 卡 → 整组 RUNNING
curl -s -X POST $B/api/jobs -H 'Content-Type: application/json' \
  -d '{"jobId":"arr1","name":"阵列","arraySize":4,"cpus":4,"memoryMb":512,
       "gpus":1,"durationLimitMs":60000}'

# 放不下 → 排队且 queueReason=INSUFFICIENT_FOR_GANG（不预留任何资源）
curl -s -X POST $B/api/jobs -H 'Content-Type: application/json' \
  -d '{"jobId":"wait","name":"等待","arraySize":2,"cpus":8,"durationLimitMs":1000}'

# 节点失联 → 整组 RETRY_WAIT；退避后恢复节点 → 整组重新派发（attempt=2）
curl -s -X POST "$B/api/nodes/n3/down?lost=true"
sleep 1.2
curl -s -X POST $B/api/schedule
curl -s -X POST $B/api/nodes/n3/recover

# 完成与重复完成（第二次幂等，资源只释放一次）
curl -s -X POST $B/api/tasks/arr1.0/complete
curl -s -X POST $B/api/tasks/arr1.0/complete
```

### 1.2 拒绝原因（可区分）

提交返回 200（接受/重复）或 **422**（拒绝，响应体带 `rejectCode`）：

| rejectCode | 触发条件 |
|---|---|
| `NAME_MISSING` | jobId / name 为空 |
| `INVALID_ARRAY_SIZE` | arraySize < 1 |
| `NEGATIVE_RESOURCE` | CPU/内存/GPU 任一维度为负 |
| `INVALID_DURATION_LIMIT` | durationLimitMs ≤ 0 |
| `EXCEEDS_MAX_NODE` | 单任务请求超过集群中**任一**节点容量，永远无法满足 |
| `EXCEEDS_CLUSTER_CAPACITY` | 阵列总请求超过集群**总容量**，整组永远无法满足 |

其它状态类错误：`409 STATE_CONFLICT`（终态后取消、完成与取消竞争落败等）、
`404 NOT_FOUND`、`400 INVALID_REQUEST`。**任何错误都不改变既有状态（失败不污染）。**

---

## 2. 资源模型

* **资源向量 `Resources(cpus, memoryMb, gpus)`**：不可变值对象，支持 `fitsIn / plus / minus / times`。
* **节点 `ClusterNode`**：固定 `capacity`，维护 `used`、`state(UP/LOST/DOWN)`、最后心跳时间。
  容量检查与分配在同一临界区内完成，`release` 带有**禁止扣成负值**的防线（重复释放会直接抛错）。
* **作业 `Job` / 任务槽 `Task`**：单作业即 `arraySize=1`。每个任务槽记录
  `state / attemptNo / nodeId / startedAt / lastHeartbeat / failReason`。
  终态 `COMPLETED / CANCELLED / FAILED` 不可逆。

状态：

```
QUEUED ──整组派发成功──> RUNNING ──全部任务完成──────────────> COMPLETED
  ^                       │  │
  │退避结束重新排队        │  ├──取消(整组)──────────────────> CANCELLED
  └──── RETRY_WAIT <──────┘  └──心跳超时/节点失效且还有重试次数
             │
             └──重试次数用尽────────────────────────────────> FAILED
```

---

## 3. 队列与整组（Gang）调度规则

1. **整组原子性**：一个阵列的**全部**任务能一次性放置才提交分配；否则一个都不分配，
   整体留在队列，绝不允许先占住一部分资源再长期空等。
2. **确定性顺序**：每轮调度按
   **有效优先级 = 静态优先级 + 等待时长 / agingBoostPerMs** 降序，
   同有效优先级时严格按 **submitSeq 升序**（更老的作业在前）。
   节点选择采用 best-fit（剩余最小、nodeId 最小破平），因此同一集群状态下结果完全确定。
3. **防饥饿**：等待越久老化补偿越高，低优先级老作业最终会超过新来的高优先级作业
   （见 `GangSchedulingTest.agingPreventsStarvationOfOldLowPriorityJob`）。
4. **排队可解释**：排队作业带 `queueReason`：
   `INSUFFICIENT_FOR_GANG`（散落后总和够但无法整组放置）、
   `NO_ONLINE_NODE`、`RETRY_BACKOFF`、`NEVER_SCHEDULED`。
5. **容量不超分**：“模拟试分配”在全局锁内进行，成功后才逐任务 `allocate` 提交；
   并发提交/调度下节点容量不可能被两个作业重复或超额分配。

容量预检在提交时完成：单任务超任一节点容量 → `EXCEEDS_MAX_NODE`；
阵列总量超集群总量 → `EXCEEDS_CLUSTER_CAPACITY`；空集群按容量不足拒绝。
预检只证明“理论可行”，当前在线资源不足时作业正常入队等待。

---

## 4. 失败与重试语义

* **节点失联/下线**：节点标记 `LOST/DOWN`，其用量立即清零（任务已不存活，清零不属于释放）；
  其上运行任务以**作业**为单位整组失败转移：同阵列其余运行任务一并回收，
  作业进入 `RETRY_WAIT` 并设置退避截止时间。
* **工作进程心跳超时**：节点仍在线但任务心跳超时，同样整组回收+整组重试。
* **重试不重复计资源**：节点失效走“清零+重分配”，心跳超时走“整组回收+重分配”，
  旧尝试的资源绝不会与新尝试叠加计费（见 `FailureRetryTest`）。
* **重试预算**：`attemptNo` 首次为 1，每次重新调度 +1；当 `attemptNo-1 > maxRetries`
  时整组 `FAILED`。进程重启恢复**不消耗**重试预算（attemptNo 保留、不额外增加）。
* **退避**：`retryBackoffMs`（可按作业覆盖，默认见配置）；退避期间为 `RETRY_WAIT`，
  到期自动转回 `QUEUED` 参与下一轮调度。

### 4.1 重复上报与取消/完成竞争

* **重复心跳**：非 `RUNNING` 任务的心跳一律幂等忽略。
* **重复完成**：已 `COMPLETED` 任务的完成上报幂等返回，**不再释放资源**。
* **取消 vs 完成**：两者竞争同一把全局锁，先到者把任务原子翻到自己的终态：
  * 完成先到 → 任务 COMPLETED、资源释放；迟到取消得到 409，终态保持 COMPLETED。
  * 取消先到 → 任务 CANCELLED、资源释放；迟到完成得到 409，**绝不再第二次扣减用量**。
  * 阵列混合终态按 `FAILED > CANCELLED > COMPLETED` 聚合，作业收敛到唯一终态。
* **旧尝试的迟到上报**：失联/超时后旧 attempt 的完成上报一律 409，不污染新尝试状态。

---

## 5. 持久化与重启恢复

* 每次状态变更把整份快照（节点、作业、任务、提交序号水位）写入本地 JSON：
  `scheduler.snapshot-file`（默认 `data/scheduler-snapshot.json`）。
* 写入采用临时文件 + 原子 rename，崩溃后要么旧快照要么新快照，不会读到半截文件。
* 重启恢复时：
  * 终态作业（COMPLETED/CANCELLED/FAILED）保持终态；
  * 崩溃前 `RUNNING/RETRY_WAIT` 的任务统一回到 `QUEUED` 重新参与整组调度，
    **不增加失败计数、不消耗重试预算**；
  * 节点上可能残留的“幽灵用量”统一归零，重新调度时重新分配，用量不会翻倍。

---

## 6. 并发模型

`SchedulerEngine` 是全部可变状态的唯一持有者，用一把公平 `ReentrantLock` 把
“校验 → 容量检查/试分配 → 状态翻转 → 落盘”串行化；可重入锁允许内部串联调度与持久化。
锁内只做内存操作与本地文件写，粒度小、无远程调用。后台 `SchedulerTicker` 与
HTTP 入口、手动触发接口走的是同一份引擎代码，因此行为一致；测试可通过
`scheduler.tick-enabled=false` 关闭后台驱动，改用 `/api/schedule`、
`/api/detect-timeouts` 获得确定性。

---

## 7. 配置项（application.properties）

| 配置 | 默认 | 含义 |
|---|---|---|
| `scheduler.snapshot-file` | `data/scheduler-snapshot.json` | 本地快照路径 |
| `scheduler.heartbeat-timeout-ms` | 15000 | 工作进程心跳超时 |
| `scheduler.node-timeout-ms` | 20000 | 节点心跳超时 |
| `scheduler.tick-interval-ms` | 2000 | 后台巡检周期 |
| `scheduler.aging-boost-per-ms` | 5000 | 每等待多久有效优先级 +1 |
| `scheduler.default-max-retries` | 2 | 默认最大重试次数 |
| `scheduler.default-backoff-ms` | 1000 | 默认重试退避 |
| `scheduler.tick-enabled` | true | 后台巡检开关 |
| `scheduler.demo-cluster` | false | 启动时注册 3 个演示节点 |

---

## 8. 测试地图

| 测试类 | 覆盖异常链路 |
|---|---|
| `SubmissionValidationTest` | 4 类非法输入可区分拒绝、超单节点/集群容量、重复提交幂等、排队原因、空集群 |
| `GangSchedulingTest` | 整组一次性派发、零预留、优先级顺序、老化防饥饿、跨轮次结果确定性 |
| `FailureRetryTest` | 节点失联整组重试、心跳超时、用量不重复计、重试耗尽失败、迟到完成被拒、离线节点 |
| `IdempotencyRaceTest` | 重复完成幂等、20 轮取消/完成并发、并发重复提交、40 作业混合并发不超额释放 |
| `CancelCompleteInterleaveTest` | 两个方向严格交错的终态竞争、混合终态恰好释放一次 |
| `RecoveryPersistenceTest` | 快照恢复、运行任务重排队、无幽灵用量、重调度不翻倍、终态保持 |
| `SchedulerWebTest` | 端到端 HTTP：422/409/404 错误归类、幂等、失联恢复全链路 |

测试日志（SLF4J，默认 INFO）会打印**关键输入**与**判定依据**，例如有效优先级计算、
整组放置失败于哪个任务、重试 attempt 变化、竞争收敛的终态与释放次数，便于复盘。

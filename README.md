# 本地集群作业调度器（Local HPC Job Scheduler）

纯本地运行的集群作业调度能力演示：用内存资源模型模拟**计算节点**与**工作进程（attempt）**，
不连接真实集群、外部服务或真实凭据。作业从提交到结束的整个生命周期，在资源竞争、节点掉线、
重复上报与并发操作下，都能得到**一致且可解释**的结果。

- 技术栈：Java 21、Spring Boot 3.4（spring-boot-starter-web / test）、JUnit 5、Jackson；
  无数据库、无消息队列、无外部依赖，持久化仅为工作目录下一个**追加式 JSON 行事件日志**。
- 构建：`mvn package`；测试：`mvn test`（27 个用例，覆盖所有要求的异常链路）。

---

## 1. 资源与队列模型

### 1.1 资源请求

每个任务的请求为 `ResourceRequest(cores, memoryMb, gpus, durationLimitSec)`：

| 维度 | 约束 | 非法时的拒绝原因（HTTP 422 `reason`） |
| --- | --- | --- |
| 计算核数 `cores` | 必须 `> 0` | `INVALID_CORES` |
| 内存 `memoryMb` | 必须 `> 0`（MB） | `INVALID_MEMORY` |
| 加速卡 `gpus` | 必须 `>= 0` | `INVALID_GPUS` |
| 时长上限 `durationLimitSec` | 必须 `> 0`（秒） | `INVALID_DURATION` |

此外在提交时做两类**永久不可满足**判定，直接拒绝、不落任何资源：

- `EXCEEDS_CLUSTER_CAPACITY`：单作业（或阵列整组总需求）超过所有在线节点容量之和；
- `EXCEEDS_NODE_CAPACITY`：阵列单个任务超过任意一个节点，永远无处放置。

**暂时不满足**（总量够但当前被占用/碎片化）不拒绝，作业进入队列并写入可解释的 `queueReason`。

### 1.2 节点与工作进程

- `ComputeNode`：总容量（核/内存/卡）、当前已分配量、在其上运行的 `attemptId` 集合、
  `ONLINE/LOST` 状态。
- `WorkerAttempt`：一次工作进程运行尝试，含 `attemptId`、`attemptNo`、所在节点、
  启动时间、最近心跳时间。**重试产生新 attempt**，资源按 attempt 精确记账。
- 容量记账规则：`allocate` 超额直接抛错、重复 attemptId 幂等；`release` 重复释放幂等，
  且释放后用量绝不允许为负。所有节点记账变更只发生在调度器全局锁临界区内。

### 1.3 队列与确定性顺序

排队中的调度单元是"单作业"或"作业阵列"。同一时刻的调度结果完全确定，排序键依次为：

1. **有效优先级降序** = 静态 `priority` + 老化提升（见下）；
2. **提交时间升序**（先到先得）；
3. 同刻提交时按**全局单调提交序号**（`submitSeq`，与随机 UUID 无关）。

**防饥饿（aging）**：作业每等待 `scheduler.starvation-boost-millis`（默认 10s），
有效优先级 +1，封顶 `scheduler.max-boost`（默认 1,000,000）。因此长期等待的低优先级作业
最终会反超后来的高优先级作业被调度。

### 1.4 并发模型

- 调度器维护一把全局锁；提交、取消、心跳、完成、节点掉线、每轮调度都是
  "**检查 + 状态变更 + 事件落盘**"在同一临界区内完成，线性化执行。
- 后台每 `scheduler.tick-interval-millis`（默认 1s）执行一轮；也可随时
  `POST /api/scheduler/tick` 手动驱动（测试全部手动驱动，结果可复现）。

---

## 2. 整组调度规则（Gang Scheduling）

一个作业阵列（`taskCount` 个同构任务）遵循 **all-or-nothing**：

1. 每轮 tick 按 1.3 的确定性顺序处理每个组；
2. 先做**影子放置（shadow placement）**：在临时账本上按"节点 id 升序"为每个任务选节点，
   节点已含本轮影子分配量；
3. **任一任务放不下 → 整组放弃，一个核都不分配**，所有任务写入同一条排队原因
   （含瓶颈维度：`CPU/内存/加速卡（请求/空闲）` 或 `整组分散放置不可行(碎片)`）；
4. 全部放得下 → 在同一临界区内一次性真正落账，所有任务同时转 `RUNNING`。

因此不会出现"先占住一部分资源再长期空等"的情况，并发调度下节点容量也不会被重复/超额分配。

---

## 3. 失败、重试与终态语义

### 3.1 心跳超时与节点失效

- 工作进程启动后以心跳保活；静默超过 `scheduler.heartbeat-timeout-millis`（默认 5s）即判定丢失。
  心跳超时会同时把所在节点推断为 `LOST`，避免重试被调回"看似在线实则失联"的节点。
- `POST /api/node-lost/{nodeId}` 模拟节点直接失效（幂等：重复通知返回 0，不重复收敛）。
- 收敛动作在一个临界区内完成：节点置 LOST → 精确释放其上每个 attempt 的资源（仅一次）
  → 逐个作业按下表处理。

### 3.2 重试或终止

| 条件 | 结果 |
| --- | --- |
| `attemptCount <= maxRetries`（默认最大重试 2 次） | 作业回 `PENDING`，写排队原因 `等待重试(k/max): ...`，可按 `scheduler.retry-backoff-millis` 退避；下一轮以**新 attempt** 重新调度，旧 attempt 资源已释放，**不重复计资源** |
| `attemptCount > maxRetries` | 作业终态 `FAILED`，成因 `MAX_RETRIES_EXHAUSTED`；阵列中出现不可重试失败时记录 `ARRAY_FAILED` |
| 作业已是终态 | 丢失事件只清理资源，**状态不倒退** |
| 丢失的是旧 attempt（迟到事件） | 忽略状态推进，仅清理索引 |

### 3.3 终态唯一性（取消 vs 完成）

- `COMPLETED`（工作进程上报）、`CANCELLED`（客户端取消）、`FAILED`（重试耗尽）是仅有的终态。
- 取消与完成无论谁先到达，都**收敛到第一个落定的终态**，后到操作幂等返回、状态不倒退；
  资源在两条路径上都只会被释放一次。
- **重复完成上报 / 重复心跳 / 旧 attempt 的迟到上报**：全部幂等忽略，不改状态、不重复释放资源、
  不报错污染已有状态（重复提交同样按 `DUPLICATE_SUBMISSION` 拒绝，不新建作业、不重复占资源）。

### 3.4 进程重启恢复（本地持久化）

- 所有状态变化先追加到 `data/scheduler-events.log`（每行一个 JSON 事件：
  `NODE_REGISTERED / NODE_LOST / JOB_SUBMITTED / ARRAY_SUBMITTED / ATTEMPT_STARTED /
  ATTEMPT_RELEASED / JOB_REQUEUED / JOB_COMPLETED / JOB_CANCELLED / JOB_FAILED / ...`）。
- 进程重启时重放日志重建内存：节点容量、作业/阵列、去重键、终态与成因全部还原；
  崩溃前仍 `RUNNING` 的 attempt 成为**孤儿**（工作进程随重启消失），统一按丢失策略
  回队重试或终止，并做资源对账（用量不得为负，记账与运行中作业严格一致）。
- 事件只追加、序号单调，重复重放结果一致。

---

## 4. 本地验证方法

### 4.1 运行测试

```bash
mvn test
```

27 个用例（日志中打印关键输入与判定依据，前缀 `[关键输入]` / `[判定依据]`）：

| 测试类 | 覆盖链路 |
| --- | --- |
| `JobValidationTests` | 4 类非法资源、超集群/单节点容量、作业与阵列重复提交（拒绝不占资源） |
| `GangSchedulingTests` | 阵列 all-or-nothing、碎片不部分占用、排队原因、优先级 + 老化防饥饿、重复 tick 确定性 |
| `FailureRetryTests` | 掉线后重试不双倍记账、重试耗尽 FAILED、心跳超时同构收敛、重复掉线幂等 |
| `TerminalRaceTests` | 完成/取消两个方向的竞争、重复完成、重复/过期心跳、旧 attempt 迟到完成、取消排队作业 |
| `ConcurrencyTests` | 16 线程并发提交 + 交错 tick 无超额、20 线程重复提交恰一份成功、8 作业并发取消/完成收敛 |
| `RecoveryTests` | 崩溃后孤儿收敛、终态/容量/去重还原、重复重放一致、阵列恢复后同进同退、恢复后继续运行 |
| `RestApiIntegrationTests` | HTTP 全链路 + 错误原因经 422 可区分 |

### 4.2 启动应用手工验证

```bash
mvn package -DskipTests
java -jar target/local-hpc-job-scheduler-java-1.0-SNAPSHOT.jar --server.port=18080
```

首次启动在空事件日志上播种 3 个演示节点：
`node-a`(8核/16GB/2GPU)、`node-b`(8核/16GB/2GPU)、`node-c`(4核/8GB/0GPU)。

REST 接口（均为本地 JSON over HTTP）：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/nodes` | 注册节点 `{nodeId, cores, memoryMb, gpus}`（幂等） |
| `GET  /api/nodes` | 节点状态与实时用量 |
| `POST /api/node-lost/{nodeId}?reason=` | 模拟节点失效并立即收敛 + 调度一轮 |
| `POST /api/jobs` | 提交作业 `{clientId, cores, memoryMb, gpus, durationLimitSec, priority, maxRetries}` |
| `POST /api/arrays` | 提交阵列（多一个 `taskCount`） |
| `GET  /api/jobs/{jobId}` | 作业状态、节点、attempt、排队原因、终态成因 |
| `POST /api/jobs/{jobId}/cancel` | 取消（body 可选 `{"reason":"..."}`） |
| `POST /api/jobs/{jobId}/heartbeat` | 心跳 `{"attemptId":"..."}` |
| `POST /api/jobs/{jobId}/complete` | 完成上报 `{"attemptId":"..."}` |
| `POST /api/scheduler/tick` | 手动跑一轮调度，返回本轮启动任务数 |

业务错误统一返回 HTTP 422：`{"error":"scheduler_error","reason":"<可区分原因>","message":"..."}`。

典型复现流程：

```bash
B=http://localhost:18080/api
# 1) 非法/超容量/重复提交
curl -s -X POST $B/jobs -H 'Content-Type: application/json' \
  -d '{"clientId":"bad","cores":0,"memoryMb":1024,"gpus":0,"durationLimitSec":60}'   # INVALID_CORES
# 2) 阵列整组调度（2x8核 -> node-a/node-b 同时启动）
ARR=$(curl -s -X POST $B/arrays -H 'Content-Type: application/json' \
  -d '{"clientId":"a1","taskCount":2,"cores":8,"memoryMb":2048,"gpus":0,"durationLimitSec":60}')
curl -s -X POST $B/scheduler/tick
# 3) 节点掉线 -> 任务回队重试（注意运行中作业需 5s 内心跳，否则也会判超时）
curl -s -X POST "$B/node-lost/node-a?reason=pulled-cable"
# 4) 终态竞争
curl -s -X POST $B/jobs/<id>/cancel   -H 'Content-Type: application/json' -d '{"reason":"x"}'
curl -s -X POST $B/jobs/<id>/complete -H 'Content-Type: application/json' -d '{"attemptId":"..."}'
# 5) 杀掉进程后重新 java -jar 启动 -> 观察日志 "重放事件 N 条"，状态自动恢复
```

### 4.3 关键配置（`src/main/resources/application.properties`）

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `scheduler.heartbeat-timeout-millis` | 5000 | 心跳超时阈值 |
| `scheduler.tick-interval-millis` | 1000 | 后台巡检周期（须 >0；测试中设大值改手动驱动） |
| `scheduler.retry-backoff-millis` | 0 | 丢失后重试退避 |
| `scheduler.default-max-retries` | 2 | 默认最大重试次数 |
| `scheduler.starvation-boost-millis` | 10000 | 每等待多久优先级 +1（防饥饿） |
| `scheduler.max-boost` | 1000000 | 老化提升上限 |
| `scheduler.event-log-path` | `data/scheduler-events.log` | 本地事件日志路径 |
| `scheduler.seed-demo-nodes` | true | 仅当日志为空时播种演示节点 |

> 清理所有本地状态：停止进程后删除 `data/` 目录即可（`data/` 已在 `.gitignore` 中）。

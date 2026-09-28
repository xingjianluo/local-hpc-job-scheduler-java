package com.github.xingjianluo.localhpcjobschedulerjava.service;

import com.github.xingjianluo.localhpcjobschedulerjava.config.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.exception.ConflictException;
import com.github.xingjianluo.localhpcjobschedulerjava.exception.JobRejectedException;
import com.github.xingjianluo.localhpcjobschedulerjava.exception.NotFoundException;
import com.github.xingjianluo.localhpcjobschedulerjava.model.ClusterNode;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.model.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.NodeState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.QueueReason;
import com.github.xingjianluo.localhpcjobschedulerjava.model.RejectCode;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Task;
import com.github.xingjianluo.localhpcjobschedulerjava.store.PersistentState;
import com.github.xingjianluo.localhpcjobschedulerjava.store.SnapshotStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 调度引擎：集群全部可变状态（节点表、作业表、提交序号水位）的唯一持有者。
 *
 * <p>所有公开写操作都在同一把 {@link ReentrantLock} 内执行（可重入以便内部串联
 * 调度与持久化），把“检查可用量→整组放置→提交生效”变成原子步骤，
 * 因此并发提交/取消/心跳/完成下：</p>
 * <ul>
 *   <li>节点容量不会被两个作业重复或超额分配；</li>
 *   <li>取消与完成同时发生时，按加锁顺序收敛到唯一终态，资源只释放一次；</li>
 *   <li>重复提交按 jobId 幂等命中，重复心跳/完成按任务状态幂等忽略。</li>
 * </ul>
 *
 * <p>调度是严格的整组（gang）策略：按“有效优先级（静态优先级+老化）降序、
 * submitSeq 升序”逐个评估作业，某作业的全部任务能一次性放置才提交分配，
 * 否则不留任何预留、整体继续排队。因此同一集群状态下调度结果完全确定。</p>
 */
@Component
public class SchedulerEngine {

    private static final Logger log = LoggerFactory.getLogger(SchedulerEngine.class);

    private final SnapshotStore store;
    private final SchedulerProperties props;
    private final ReentrantLock lock = new ReentrantLock(true);

    /** 作业保持插入顺序（submitSeq 顺序），所有访问均在锁内。 */
    private final Map<String, Job> jobs = new LinkedHashMap<>();
    private final Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    /** taskId -> 所属 jobId，上报心跳/完成时定位。 */
    private final Map<String, String> taskIndex = new LinkedHashMap<>();
    private long submitSeqWatermark;

    /** 可替换时钟：测试注入可控时间以确定性地验证超时/退避/老化。 */
    private volatile java.util.function.LongSupplier clock = System::currentTimeMillis;

    /** 测试用：替换时间源，以确定性地验证超时/退避/老化。 */
    public void setClock(java.util.function.LongSupplier clock) {
        this.clock = clock;
    }

    public SchedulerEngine(SnapshotStore store, SchedulerProperties props) {
        this.store = store;
        this.props = props;
        restore();
    }

    private void restore() {
        lock.lock();
        try {
            PersistentState state = store.load();
            if (state == null) {
                return;
            }
            long now = nowMs();
            for (ClusterNode n : state.nodes()) {
                nodes.put(n.nodeId(), n);
            }
            submitSeqWatermark = state.submitSeqWatermark();
            for (Job j : state.jobs()) {
                jobs.put(j.id(), j);
                for (Task t : j.tasks()) {
                    taskIndex.put(t.id(), j.id());
                    // 进程重启：曾经在运行/重试等待的任务统一回到队列重新调度，
                    // 不增加 attemptNo/失败计数；其旧绑定节点的用量以快照为准。
                    if (t.state() == JobState.RUNNING || t.state() == JobState.RETRY_WAIT) {
                        t.recoverToQueue();
                    }
                }
                if (j.state() == JobState.RUNNING || j.state() == JobState.RETRY_WAIT) {
                    j.noteQueued(QueueReason.NEVER_SCHEDULED, "进程重启恢复，重新排队");
                }
            }
            // 恢复后节点上可能仍记录着已被重新排队任务的用量，统一归零，
            // 避免“幽灵占用”；这些任务重新调度时会重新分配。
            for (ClusterNode n : nodes.values()) {
                if (n.state() == NodeState.UP) {
                    n.recover(now);
                }
            }
            log.info("恢复完成：作业 {} 个，节点 {} 个，存活任务已重新排队",
                    jobs.size(), nodes.size());
            persist();
        } finally {
            lock.unlock();
        }
    }

    // ============================ 节点管理 ============================

    public ClusterNode registerNode(String nodeId, Resources capacity) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId 不能为空");
        }
        if (capacity == null || capacity.hasNegativeDimension()) {
            throw new IllegalArgumentException("节点容量非法: " + capacity);
        }
        lock.lock();
        try {
            ClusterNode existing = nodes.get(nodeId);
            if (existing != null) {
                if (existing.state() != NodeState.UP) {
                    existing.recover(nowMs());
                    log.info("节点 {} 重新注册并恢复为 UP，容量 {}", nodeId, capacity);
                } else {
                    log.info("节点 {} 重复注册（幂等忽略），容量保持 {}", nodeId, existing.capacity());
                }
                persist();
                return existing;
            }
            ClusterNode node = new ClusterNode(nodeId, capacity);
            node.heartbeat(nowMs());
            nodes.put(nodeId, node);
            log.info("注册节点 {}，容量 {}，初始心跳 {}", nodeId, capacity, node.lastHeartbeatMs());
            persist();
            return node;
        } finally {
            lock.unlock();
        }
    }

    /** 节点心跳：保活用（即使其上暂无任务）。 */
    public void nodeHeartbeat(String nodeId) {
        lock.lock();
        try {
            ClusterNode node = requireNode(nodeId);
            node.heartbeat(nowMs());
            persist();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 把节点标记为失联(lost=true→LOST)或主动下线(lost=false→DOWN)，
     * 并对其上所有运行任务做整组失败转移。
     */
    public ClusterNode markNodeDown(String nodeId, boolean lost) {
        lock.lock();
        try {
            ClusterNode node = requireNode(nodeId);
            if (node.state() != NodeState.UP) {
                log.info("节点 {} 已处于 {}，重复下线操作幂等忽略", nodeId, node.state());
                return node;
            }
            NodeState target = lost ? NodeState.LOST : NodeState.DOWN;
            Resources orphaned = node.markNotOnline(target);
            log.warn("节点 {} 进入 {}，其上被遗弃用量 {} 将由整组失败转移处理",
                    nodeId, target, orphaned);
            failoverTasksOnNode(nodeId,
                    lost ? "节点失联：" + nodeId : "节点主动下线：" + nodeId);
            persist();
            return node;
        } finally {
            lock.unlock();
        }
    }

    public ClusterNode recoverNode(String nodeId) {
        lock.lock();
        try {
            ClusterNode node = requireNode(nodeId);
            node.recover(nowMs());
            log.info("节点 {} 恢复在线", nodeId);
            persist();
            schedule();
            return node;
        } finally {
            lock.unlock();
        }
    }

    public ClusterNode getNode(String nodeId) {
        lock.lock();
        try {
            return requireNode(nodeId);
        } finally {
            lock.unlock();
        }
    }

    public List<ClusterNode> nodes() {
        lock.lock();
        try {
            return List.copyOf(nodes.values());
        } finally {
            lock.unlock();
        }
    }

    private ClusterNode requireNode(String nodeId) {
        ClusterNode node = nodes.get(nodeId);
        if (node == null) {
            throw new NotFoundException("节点不存在: " + nodeId);
        }
        return node;
    }

    // ============================ 作业提交 ============================

    public SubmitOutcome submitJob(String jobId, String name, int arraySize,
                                   Resources perTask, long durationLimitMs,
                                   Integer priority, Integer maxRetries,
                                   Long retryBackoffMs) {
        // 先做不依赖锁的静态校验，给出可区分原因。
        validate(jobId, name, arraySize, perTask, durationLimitMs);

        lock.lock();
        try {
            Job existing = jobs.get(jobId);
            if (existing != null) {
                log.info("重复提交 jobId={}（已有状态 {}），幂等返回，不重复占用资源",
                        jobId, existing.state());
                return new SubmitOutcome(SubmitOutcome.Kind.DUPLICATE, existing, null,
                        "作业已存在，重复提交已幂等忽略");
            }

            // 容量预检：针对整个集群当前容量（在线与否都计入理论容量；
            // 若理论上都放不下，则任何时候都放不下）。
            RejectCode capacityCode = checkCapacityFeasibility(arraySize, perTask);
            if (capacityCode != null) {
                String msg = capacityMessage(capacityCode, arraySize, perTask);
                log.warn("提交被拒绝 jobId={} 原因={} 详情：{}", jobId, capacityCode, msg);
                return new SubmitOutcome(SubmitOutcome.Kind.REJECTED, null, capacityCode, msg);
            }

            long now = nowMs();
            long seq = ++submitSeqWatermark;
            int prio = priority == null ? 0 : priority;
            int retries = maxRetries == null ? props.defaultMaxRetries() : maxRetries;
            long backoff = retryBackoffMs == null ? props.defaultBackoffMs() : retryBackoffMs;

            List<Task> tasks = new ArrayList<>(arraySize);
            for (int i = 0; i < arraySize; i++) {
                String tid = jobId + "." + i;
                tasks.add(new Task(tid, i, perTask, durationLimitMs));
                taskIndex.put(tid, jobId);
            }
            Job job = new Job(jobId, name, arraySize, perTask, durationLimitMs,
                    prio, seq, now, retries, backoff, tasks);
            jobs.put(jobId, job);
            log.info("作业已入队 jobId={} name='{}' arraySize={} request/task={} "
                            + "durationLimit={}ms priority={} seq={} 总需求={}",
                    jobId, name, arraySize, perTask, durationLimitMs, prio, seq,
                    perTask.times(arraySize));
            persist();
            // 提交后立即尝试调度：资源够则直接 RUNNING，不够则保持 QUEUED 并记录排队原因。
            schedule();
            Job refreshed = jobs.get(jobId);
            return new SubmitOutcome(SubmitOutcome.Kind.ACCEPTED, refreshed, null, "已受理");
        } finally {
            lock.unlock();
        }
    }

    private void validate(String jobId, String name, int arraySize,
                          Resources perTask, long durationLimitMs) {
        if (jobId == null || jobId.isBlank()) {
            throw new JobRejectedException(RejectCode.NAME_MISSING, "jobId 不能为空");
        }
        if (name == null || name.isBlank()) {
            throw new JobRejectedException(RejectCode.NAME_MISSING, "作业名(name)不能为空");
        }
        if (arraySize < 1) {
            throw new JobRejectedException(RejectCode.INVALID_ARRAY_SIZE,
                    "阵列大小必须 >= 1，实际: " + arraySize);
        }
        if (perTask == null || perTask.hasNegativeDimension()) {
            throw new JobRejectedException(RejectCode.NEGATIVE_RESOURCE,
                    "资源请求存在负数: " + perTask);
        }
        if (durationLimitMs <= 0) {
            throw new JobRejectedException(RejectCode.INVALID_DURATION_LIMIT,
                    "时长上限必须 > 0(ms)，实际: " + durationLimitMs);
        }
    }

    /** 容量可行性预检；返回 null 表示通过。 */
    private RejectCode checkCapacityFeasibility(int arraySize, Resources perTask) {
        if (nodes.isEmpty()) {
            return RejectCode.EXCEEDS_CLUSTER_CAPACITY;
        }
        Resources maxNode = Resources.ZERO;
        Resources total = Resources.ZERO;
        for (ClusterNode n : nodes.values()) {
            maxNode = maxNode.maxPerDimension(n.capacity());
            total = total.plus(n.capacity());
        }
        if (!perTask.fitsIn(maxNode)) {
            return RejectCode.EXCEEDS_MAX_NODE;
        }
        if (!perTask.times(arraySize).fitsIn(total)) {
            return RejectCode.EXCEEDS_CLUSTER_CAPACITY;
        }
        return null;
    }

    private String capacityMessage(RejectCode code, int arraySize, Resources perTask) {
        if (code == RejectCode.EXCEEDS_MAX_NODE) {
            return "单任务请求 " + perTask + " 超出集群中任一节点容量，永远无法满足";
        }
        return "阵列总请求 " + perTask.times(arraySize)
                + "（" + arraySize + " 个任务）超出集群总容量，整组无法满足";
    }

    public Job getJob(String jobId) {
        lock.lock();
        try {
            Job job = jobs.get(jobId);
            if (job == null) {
                throw new NotFoundException("作业不存在: " + jobId);
            }
            return job;
        } finally {
            lock.unlock();
        }
    }

    public List<Job> jobs() {
        lock.lock();
        try {
            return List.copyOf(jobs.values());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 取消作业。
     *
     * <p>取消与完成竞争同一把锁：先到者以原子方式把任务翻到 CANCELLED（完成路径翻到
     * COMPLETED）。任务状态的 CAS 式判断保证两种终态互斥，资源恰好释放一次，
     * 不会出现“完成释放一遍、取消又释放一遍”或状态倒退。</p>
     */
    public Job cancelJob(String jobId) {
        lock.lock();
        try {
            Job job = requireJob(jobId);
            if (job.isTerminal()) {
                throw new ConflictException("作业已处于终态 " + job.state()
                        + "，不能取消；终态不可逆");
            }
            int released = 0;
            for (Task t : job.tasks()) {
                if (t.isTerminal()) {
                    // 可能与完成竞争：已完成的任务保持 COMPLETED，不再触碰资源。
                    continue;
                }
                if (t.isRunning() && t.nodeId() != null) {
                    ClusterNode node = nodes.get(t.nodeId());
                    if (node != null) {
                        node.release(t.request());
                        released++;
                    }
                }
                t.markCancelled();
            }
            job.reconcileFromTasks();
            if (job.state() == JobState.COMPLETED) {
                // 竞争落败：取消到达时所有任务都已完成（完成先到并已释放资源）。
                // 终态唯一收敛为 COMPLETED，取消不再触碰资源；向调用方显式报告竞争结果。
                log.info("取消 jobId={} 与完成竞争落败：全部任务已完成，保持 COMPLETED，资源未重复释放", jobId);
                throw new ConflictException("作业 " + jobId
                        + " 的全部任务已在此刻完成，取消与完成竞争落败，终态收敛为 COMPLETED");
            }
            if (job.state() != JobState.CANCELLED) {
                // 部分任务已完成（竞争中完成先到）：作业终态由任务聚合决定。
                log.info("取消 jobId={} 时部分任务已完成，聚合终态={}", jobId, job.state());
            } else {
                job.markCancelled("用户取消；释放运行任务 " + released + " 个");
            }
            log.info("取消处理完成 jobId={} 终态={} 释放资源任务数={}", jobId, job.state(), released);
            persist();
            return job;
        } finally {
            lock.unlock();
        }
    }

    // ============================ 工作进程上报 ============================

    public void taskHeartbeat(String taskId) {
        lock.lock();
        try {
            Task task = requireTask(taskId);
            if (!task.isRunning()) {
                // 重复/过期心跳（任务已结束或尚未被调度）幂等忽略，不改变状态。
                log.info("心跳来自非运行任务 {}（状态 {}），幂等忽略", taskId, task.state());
                return;
            }
            long now = nowMs();
            task.heartbeat(now);
            ClusterNode node = nodes.get(task.nodeId());
            if (node != null) {
                node.heartbeat(now);
            }
            log.debug("心跳 task={} attempt={} node={} t={}",
                    taskId, task.attemptNo(), task.nodeId(), now);
            persist();
        } finally {
            lock.unlock();
        }
    }

    public Job taskComplete(String taskId) {
        lock.lock();
        try {
            Task task = requireTask(taskId);
            Job job = jobs.get(taskIndex.get(taskId));
            if (task.state() == JobState.COMPLETED) {
                // 重复完成上报：幂等返回，绝不第二次释放资源。
                log.info("重复完成上报 task={}（已 COMPLETED），幂等忽略", taskId);
                return job;
            }
            if (task.state() == JobState.CANCELLED) {
                // 取消与完成竞争落败：取消已先把资源恰好释放一次。
                // 完成请求必须被拒绝且绝不能再次归还资源，否则节点用量会被错误地二次扣减。
                log.info("完成上报竞争落败 task={}（任务已 CANCELLED，资源已由取消路径释放）", taskId);
                throw new ConflictException("任务 " + taskId
                        + " 已被取消，完成上报在竞争中落败；终态保持 CANCELLED，资源未重复释放");
            }
            if (task.state() == JobState.FAILED) {
                throw new ConflictException("任务 " + taskId
                        + " 已失败终态，完成上报被拒绝");
            }
            if (!task.isRunning()) {
                throw new ConflictException("任务 " + taskId + " 当前状态 " + task.state()
                        + "，不能上报完成（可能尚未被调度）");
            }
            ClusterNode node = nodes.get(task.nodeId());
            if (node == null || node.state() != NodeState.UP) {
                throw new ConflictException("任务 " + taskId + " 所在节点 " + task.nodeId()
                        + " 已不在线，完成上报被拒绝，按失败重试处理");
            }
            node.release(task.request());
            task.markCompleted();
            job.reconcileFromTasks();
            log.info("任务完成 task={} attempt={} node={}，释放 {}，作业聚合状态={}",
                    taskId, task.attemptNo(), node.nodeId(), task.request(), job.state());
            if (job.state() == JobState.COMPLETED) {
                job.markCompleted();
            }
            persist();
            // 有作业完成释放资源后，排队作业可能获得调度机会。
            schedule();
            return job;
        } finally {
            lock.unlock();
        }
    }

    // ============================ 调度（整组） ============================

    /**
     * 执行一轮整组调度。
     *
     * <p>候选作业为 QUEUED，或 RETRY_WAIT 退避已结束；按
 * “有效优先级（priority + 等待时长/agingBoostPerMs）降序，submitSeq 升序”排序，
     * 逐个尝试放置。放置失败的作业保持排队并记录可解释原因，不预留任何资源。</p>
     *
     * @return 本轮被整组派发的作业数
     */
    public int schedule() {
        lock.lock();
        try {
            long now = nowMs();
            // RETRY_WAIT 退避结束的作业先转回 QUEUED。
            for (Job j : jobs.values()) {
                if (j.retryEligible(now)) {
                    j.activateAfterBackoff(now);
                    for (Task t : j.tasks()) {
                        if (t.state() == JobState.RETRY_WAIT) {
                            t.backToQueue("退避结束，重新排队");
                        }
                    }
                    j.reconcileFromTasks();
                }
            }

            List<Job> candidates = jobs.values().stream()
                    .filter(j -> j.state() == JobState.QUEUED)
                    .sorted(schedulerOrder(now))
                    .toList();

            boolean anyOnline = nodes.values().stream().anyMatch(ClusterNode::isOnline);
            int dispatched = 0;
            for (Job job : candidates) {
                if (!anyOnline) {
                    job.noteQueued(QueueReason.NO_ONLINE_NODE, "集群当前无在线节点");
                    continue;
                }
                PlacementResult result = tryPlaceGang(job);
                if (result.ok()) {
                    commitPlacement(job, result, now);
                    dispatched++;
                    anyOnline = nodes.values().stream().anyMatch(ClusterNode::isOnline);
                } else {
                    job.noteQueued(result.reason(), result.detail());
                }
            }
            if (dispatched > 0) {
                log.info("本轮整组调度完成，派发作业 {} 个", dispatched);
                persist();
            }
            return dispatched;
        } finally {
            lock.unlock();
        }
    }

    private Comparator<Job> schedulerOrder(long now) {
        // 有效优先级降序；同分时严格按提交序号升序（更老的作业先调度，保证确定且防饥饿）。
        // 显式两级比较，避免 Comparator.reversed() 连带反转第二级。
        return (a, b) -> {
            long pa = effectivePriority(a, now);
            long pb = effectivePriority(b, now);
            if (pa != pb) {
                return Long.compare(pb, pa); // 高优先级在前
            }
            return Long.compare(a.submitSeq(), b.submitSeq()); // 同分更老在前
        };
    }

    private long effectivePriority(Job j, long now) {
        long waited = Math.max(0, now - j.createdAtMs());
        long aging = props.agingBoostPerMs() <= 0 ? 0 : waited / props.agingBoostPerMs();
        return j.priority() + aging;
    }

    private PlacementResult tryPlaceGang(Job job) {
        // 快照当前用量，计算“试分配”但不立即提交，全部任务成功才 commit。
        Map<String, Resources> simulatedUsed = new LinkedHashMap<>();
        Map<String, Resources> caps = new LinkedHashMap<>();
        List<String> onlineNodes = new ArrayList<>();
        for (ClusterNode n : nodes.values()) {
            if (n.isOnline()) {
                onlineNodes.add(n.nodeId());
                simulatedUsed.put(n.nodeId(), n.used());
                caps.put(n.nodeId(), n.capacity());
            }
        }
        if (onlineNodes.isEmpty()) {
            return PlacementResult.fail(QueueReason.NO_ONLINE_NODE, "无在线节点");
        }

        Map<String, String> assignment = new LinkedHashMap<>();
        for (Task t : job.tasks()) {
            String chosen = chooseBestFit(t.request(), onlineNodes, simulatedUsed, caps);
            if (chosen == null) {
                Resources totalAvail = Resources.ZERO;
                for (String nid : onlineNodes) {
                    totalAvail = totalAvail.plus(caps.get(nid).minus(simulatedUsed.get(nid)));
                }
                String detail = "整组放置失败于任务 #" + t.index() + "（需求 " + t.request()
                        + "）；在线剩余合计 " + totalAvail
                        + "，但无法在单节点上放下该任务并同时满足整组，整体等待（不预留资源）";
                return PlacementResult.fail(QueueReason.INSUFFICIENT_FOR_GANG, detail);
            }
            simulatedUsed.put(chosen, simulatedUsed.get(chosen).plus(t.request()));
            assignment.put(t.id(), chosen);
        }
        return PlacementResult.ok(assignment);
    }

    /** 最优匹配（best-fit）：在能放下的在线节点中选剩余量最小且 nodeId 最小者，保证确定性。 */
    private String chooseBestFit(Resources req, List<String> onlineNodes,
                                 Map<String, Resources> simulatedUsed,
                                 Map<String, Resources> caps) {
        String best = null;
        Resources bestLeft = null;
        for (String nid : onlineNodes) {
            Resources left = caps.get(nid).minus(simulatedUsed.get(nid));
            if (req.fitsIn(left)) {
                if (best == null
                        || compareLeft(left, bestLeft) < 0
                        || (compareLeft(left, bestLeft) == 0 && nid.compareTo(best) < 0)) {
                    best = nid;
                    bestLeft = left;
                }
            }
        }
        return best;
    }

    private long compareLeft(Resources a, Resources b) {
        // 以 CPU 剩余为主键、内存次之、GPU 再次；仅用于确定性挑选，不具业务语义。
        if (a.cpus() != b.cpus()) {
            return Long.compare(a.cpus(), b.cpus());
        }
        if (a.memoryMb() != b.memoryMb()) {
            return Long.compare(a.memoryMb(), b.memoryMb());
        }
        return Long.compare(a.gpus(), b.gpus());
    }

    private void commitPlacement(Job job, PlacementResult result, long now) {
        for (Task t : job.tasks()) {
            String nid = result.assignment().get(t.id());
            ClusterNode node = nodes.get(nid);
            // 此处的 allocate 是整组原子提交的一部分：simulation 已验证可行，
            // 且同一时刻没有其他线程能改动节点用量（全局锁）。
            node.allocate(t.request());
            t.markScheduled(nid, now);
        }
        job.markDispatched();
        log.info("整组派发 jobId={} arraySize={} 放置：{}", job.id(), job.arraySize(),
                describePlacement(result.assignment()));
    }

    private String describePlacement(Map<String, String> assignment) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : assignment.entrySet()) {
            if (!sb.isEmpty()) {
                sb.append(", ");
            }
            sb.append(e.getKey()).append("->").append(e.getValue());
        }
        return sb.toString();
    }

    // ============================ 超时检测与失败转移 ============================

    /**
     * 巡检：先检测失联节点（整节点失败转移），再检测单任务心跳超时。
     *
     * @return 受影响（进入重试等待）的作业数
     */
    public int detectTimeouts() {
        lock.lock();
        try {
            long now = nowMs();
            int affected = 0;

            // 1) 节点级失联：在线但节点心跳超时。
            for (ClusterNode node : List.copyOf(nodes.values())) {
                if (node.state() == NodeState.UP
                        && now - node.lastHeartbeatMs() > props.nodeTimeoutMs()) {
                    log.warn("节点心跳超时 node={} 超过 {}ms，判定 LOST",
                            node.nodeId(), props.nodeTimeoutMs());
                    Resources orphaned = node.markNotOnline(NodeState.LOST);
                    log.warn("节点 {} 被遗弃用量 {} 转整组失败转移", node.nodeId(), orphaned);
                    failoverTasksOnNode(node.nodeId(),
                            "节点心跳超时失联：" + node.nodeId());
                    affected++;
                }
            }

            // 2) 任务级心跳超时：节点仍在线，但工作进程超时。
            for (Job job : List.copyOf(jobs.values())) {
                for (Task t : job.tasks()) {
                    if (t.isRunning()
                            && now - t.lastHeartbeatMs() > props.heartbeatTimeoutMs()) {
                        log.warn("任务心跳超时 task={} node={} attempt={} 超过 {}ms",
                                t.id(), t.nodeId(), t.attemptNo(), props.heartbeatTimeoutMs());
                        // 资源回收由 failoverJob 整组统一完成，避免对超时任务双重归还。
                        failoverJob(job,
                                "工作进程心跳超时：" + t.id() + "（attempt " + t.attemptNo() + "）");
                        affected++;
                        break; // 该作业已整组处理，跳出任务循环
                    }
                }
            }
            if (affected > 0) {
                persist();
                // 重试退避为 0 或有其它排队作业时，失败收敛后立即尝试重新调度。
                schedule();
            }
            return affected;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 节点失效导致其上所有运行任务失败：以作业为单位做整组失败转移。
     * 节点用量已在 markNotOnline 中清零，所以这里不再逐任务 release，
     * 从根上杜绝“重试重复计入资源用量”。
     */
    private void failoverTasksOnNode(String lostNodeId, String reason) {
        for (Job job : jobs.values()) {
            boolean impacted = false;
            for (Task t : job.tasks()) {
                if (t.isRunning() && lostNodeId.equals(t.nodeId())) {
                    impacted = true;
                    break;
                }
            }
            if (impacted) {
                failoverJob(job, reason);
            }
        }
    }

    /**
     * 把一个作业整组收敛到“重试等待”或“失败”：
     * 释放（或遗弃）所有运行任务的绑定，未超过最大重试次数则 RETRY_WAIT，
     * 否则整个作业 FAILED（任一任务不可重试即整组失败）。
     */
    private void failoverJob(Job job, String reason) {
        if (job.isTerminal()) {
            return;
        }
        // 整组语义：一个任务失败，同阵列其余运行任务一并回收，
        // 之后整个作业整组重试；这里同时完成用量回收，避免幽灵占用。
        // 已失联/下线节点的用量已在 markNotOnline 清零，不再逐任务归还。
        int nextAttempt = 0;
        boolean anyExhausted = false;
        for (Task t : job.tasks()) {
            if (t.isRunning() || t.state() == JobState.RETRY_WAIT) {
                if (t.attemptNo() + 1 > job.maxRetries() + 1) {
                    anyExhausted = true;
                }
                nextAttempt = Math.max(nextAttempt, t.attemptNo() + 1);
                if (t.isRunning()) {
                    ClusterNode holder = nodes.get(t.nodeId());
                    if (holder != null && holder.state() == NodeState.UP) {
                        holder.release(t.request());
                    }
                }
            }
        }
        if (anyExhausted) {
            for (Task t : job.tasks()) {
                if (!t.isTerminal()) {
                    t.markFailed(reason + "；重试次数已用尽");
                }
            }
            job.reconcileFromTasks();
            job.markFailed(reason + "；重试次数已用尽，整组失败");
            log.error("作业失败 jobId={} 原因={}", job.id(), reason);
            return;
        }
        for (Task t : job.tasks()) {
            if (t.isRunning() || t.state() == JobState.QUEUED || t.state() == JobState.RETRY_WAIT) {
                t.markRetryWait(reason);
            }
        }
        job.reconcileFromTasks();
        job.markRetryWait(nowMs(), reason + "；整组将在退避后重新调度（下次 attempt="
                + nextAttempt + "）");
        log.warn("作业进入重试等待 jobId={} 原因={} 退避={}ms",
                job.id(), reason, job.retryBackoffMs());
    }

    // ============================ 基础设施 ============================

    private Job requireJob(String jobId) {
        Job job = jobs.get(jobId);
        if (job == null) {
            throw new NotFoundException("作业不存在: " + jobId);
        }
        return job;
    }

    private Task requireTask(String taskId) {
        String jobId = taskIndex.get(taskId);
        if (jobId == null) {
            throw new NotFoundException("任务不存在: " + taskId);
        }
        Job job = jobs.get(jobId);
        return job.tasks().stream()
                .filter(t -> t.id().equals(taskId))
                .findFirst()
                .orElseThrow(() -> new NotFoundException("任务不存在: " + taskId));
    }

    private void persist() {
        if (store == null) {
            return;
        }
        store.save(new PersistentState(
                List.copyOf(nodes.values()),
                List.copyOf(jobs.values()),
                submitSeqWatermark));
    }

    public long nowMs() {
        return clock.getAsLong();
    }

    /** 内部：调度放置结果。 */
    private record PlacementResult(boolean ok, QueueReason reason, String detail,
                                   Map<String, String> assignment) {
        static PlacementResult ok(Map<String, String> assignment) {
            return new PlacementResult(true, null, null, Map.copyOf(assignment));
        }

        static PlacementResult fail(QueueReason reason, String detail) {
            return new PlacementResult(false, reason, detail, Map.of());
        }
    }

    /** 提交结果（接受/重复/拒绝），供上层服务翻译成 HTTP 响应。 */
    public record SubmitOutcome(Kind kind, Job job, RejectCode rejectCode, String message) {
        public enum Kind {ACCEPTED, DUPLICATE, REJECTED}
    }
}

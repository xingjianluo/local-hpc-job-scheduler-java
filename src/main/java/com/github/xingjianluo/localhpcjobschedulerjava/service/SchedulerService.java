package com.github.xingjianluo.localhpcjobschedulerjava.service;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerState;
import com.github.xingjianluo.localhpcjobschedulerjava.core.TimeService;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ComputeNode;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobArray;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.NodeState;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ResourceRequest;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.WorkerAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 核心调度循环（每次 tick 一把全局锁）：
 * 1) 收敛：检测心跳超时的 RUNNING 尝试，按失败策略重试或终止（重试不重复计资源）；
 * 2) 调度：按"有效优先级（含防饥饿老化）降序、提交时间升序、id 升序"的确定性顺序，
 *    对阵列/单作业做整组放置——所有任务在同一临界区内一次性全部分配成功，
 *    否则一个都不分配并写明排队原因，绝不允许先占部分资源再长期空等。
 */
public class SchedulerService {

    private static final Logger log = LoggerFactory.getLogger(SchedulerService.class);

    private final SchedulerState state;
    private final TimeService time;
    private final SchedulerProperties props;

    public SchedulerService(SchedulerState state, TimeService time, SchedulerProperties props) {
        this.state = state;
        this.time = time;
        this.props = props;
    }

    /** 执行一轮调度，返回本轮新启动的任务数。 */
    public int tick() {
        synchronized (state.lock()) {
            long now = time.nowMillis();
            detectHeartbeatTimeouts(now);

            // 以 arrayId 聚合待调度单元（无阵列的作业各自成组），保持提交顺序稳定
            Map<String, List<Job>> groups = new LinkedHashMap<>();
            List<String> standalone = new ArrayList<>();
            for (Job job : state.pendingJobs()) {
                if (job.readyAtMillis() > now) {
                    job.queueReason("重试退避中，还需等待 " + (job.readyAtMillis() - now) + "ms");
                    continue;
                }
                if (job.arrayId() == null) {
                    groups.put(job.id(), new ArrayList<>(List.of(job)));
                    standalone.add(job.id());
                } else {
                    groups.computeIfAbsent(job.arrayId(), k -> new ArrayList<>()).add(job);
                }
            }

            // 阵列任务按 index 排序，保证放置与解释的确定性
            for (List<Job> tasks : groups.values()) {
                if (tasks.size() > 1 || (tasks.get(0).arrayId() != null)) {
                    tasks.sort(Comparator.comparingInt(Job::arrayIndex));
                }
            }

            List<Map.Entry<String, List<Job>>> ordered = new ArrayList<>(groups.entrySet());
            ordered.sort((a, b) -> compareGroups(a.getValue(), b.getValue(), now));

            int started = 0;
            for (Map.Entry<String, List<Job>> entry : ordered) {
                started += scheduleGroup(entry.getKey(), entry.getValue(), now);
            }
            if (started > 0) {
                log.info("调度轮次完成: 新启动任务 {} 个", started);
            }
            return started;
        }
    }

    // ============================ 心跳超时收敛 ============================

    private void detectHeartbeatTimeouts(long now) {
        List<Job> running = new ArrayList<>();
        for (Job job : state.jobs().values()) {
            if (job.state() == JobState.RUNNING && job.currentAttempt() != null) {
                running.add(job);
            }
        }
        for (Job job : running) {
            WorkerAttempt attempt = job.currentAttempt();
            ComputeNode node = state.nodes().get(attempt.nodeId());
            if (node != null && node.state() == NodeState.LOST) {
                // 节点掉线由 markNodeLost 收敛，这里兜底（理论上不会走到）
                continue;
            }
            long silent = now - attempt.lastHeartbeatMillis();
            if (silent > props.getHeartbeatTimeoutMillis()) {
                log.info("心跳超时判定: job={}, attempt={}, node={}, 静默={}ms > 阈值={}ms",
                        job.id(), attempt.attemptId(), attempt.nodeId(), silent,
                        props.getHeartbeatTimeoutMillis());
                state.eventLog().append("HEARTBEAT_TIMEOUT", now, job.id(),
                        attempt.nodeId(), attempt.attemptId(),
                        List.of(String.valueOf(silent)));
                // 心跳超时意味着该工作进程（及其节点通道）已失联：节点置 LOST，
                // 避免重试被再次调回同一"看似在线实则失联"的节点；重复判定幂等。
                if (node != null && node.state() == NodeState.ONLINE) {
                    node.state(NodeState.LOST);
                    state.eventLog().append("NODE_LOST", now, null, attempt.nodeId(), null,
                            List.of("心跳超时 " + silent + "ms 推断节点失联"));
                    log.info("节点 {} 因心跳超时被推断为 LOST", attempt.nodeId());
                }
                FailurePolicy.convergeLostAttempt(state, time, props, job,
                        attempt.attemptId(), attempt.nodeId(),
                        "心跳超时 " + silent + "ms");
            }
        }
    }

    // ============================ 整组调度 ============================

    /**
     * 尝试整组放置。全部成功才提交分配；任一任务放不下则整体等待，
     * 不修改任何节点用量（gang 原子性）。
     *
     * @return 实际启动的任务数（0 或组大小）
     */
    private int scheduleGroup(String groupId, List<Job> tasks, long now) {
        boolean isArray = tasks.get(0).arrayId() != null;
        // 先做"影子放置"：仅在临时账本上试分配，成功后才真正落账
        Map<String, long[]> shadow = new LinkedHashMap<>(); // nodeId -> [cores, mem, gpus]
        List<String[]> placement = new ArrayList<>();       // [jobId, nodeId, attemptId]

        for (Job job : tasks) {
            ResourceRequest req = job.request();
            String chosen = chooseNode(req, shadow);
            if (chosen == null) {
                explainQueue(groupId, tasks, job, isArray);
                return 0;
            }
            long[] usage = shadow.computeIfAbsent(chosen, k -> {
                ComputeNode n = state.nodes().get(k);
                return new long[]{n.usedCores(), n.usedMemoryMb(), n.usedGpus()};
            });
            usage[0] += req.cores();
            usage[1] += req.memoryMb();
            usage[2] += req.gpus();
            placement.add(new String[]{job.id(), chosen,
                    "attempt-" + UUID.randomUUID()});
        }

        // 影子放置全部成功：在同一临界区内一次性真正落账
        for (int i = 0; i < tasks.size(); i++) {
            Job job = tasks.get(i);
            String[] p = placement.get(i);
            String nodeId = p[1];
            String attemptId = p[2];
            ComputeNode node = state.nodes().get(nodeId);
            int attemptNo = job.attemptCount() + 1;
            job.attemptCount(attemptNo);
            WorkerAttempt attempt = new WorkerAttempt(attemptId, attemptNo, nodeId, now, now);
            node.allocate(attemptId, job.id(), job.request());
            state.attemptIndex().put(attemptId, job.id());
            job.currentAttempt(attempt);
            job.state(JobState.RUNNING);
            job.queueReason(null);
            job.readyAtMillis(now);
            state.eventLog().append("ATTEMPT_STARTED", now, job.id(), nodeId, attemptId,
                    List.of(String.valueOf(attemptNo), String.valueOf(job.request().cores()),
                            String.valueOf(job.request().memoryMb()),
                            String.valueOf(job.request().gpus())));
            log.info("启动尝试: job={}, attempt={}(#{}), node={}, 分配={}核/{}MB/{}卡",
                    job.id(), attemptId, attemptNo, nodeId, job.request().cores(),
                    job.request().memoryMb(), job.request().gpus());
        }
        if (isArray) {
            log.info("阵列 {} 整组调度成功，共 {} 个任务同时获得资源", groupId, tasks.size());
        }
        return tasks.size();
    }

    /**
     * 在影子账本下选择可容纳 req 的节点：
     * 在线、考虑本轮已影子分配量，按节点 id 升序（确定性）取第一个可放入的。
     */
    private String chooseNode(ResourceRequest req, Map<String, long[]> shadow) {
        List<ComputeNode> nodes = new ArrayList<>(state.nodes().values());
        nodes.sort(Comparator.comparing(ComputeNode::id));
        for (ComputeNode node : nodes) {
            if (node.state() != NodeState.ONLINE) {
                continue;
            }
            long[] extra = shadow.get(node.id());
            long usedCores = node.usedCores() + (extra == null ? 0 : extra[0]);
            long usedMem = node.usedMemoryMb() + (extra == null ? 0 : extra[1]);
            long usedGpus = node.usedGpus() + (extra == null ? 0 : extra[2]);
            if (req.cores() <= node.totalCores() - usedCores
                    && req.memoryMb() <= node.totalMemoryMb() - usedMem
                    && req.gpus() <= node.totalGpus() - usedGpus) {
                return node.id();
            }
        }
        return null;
    }

    /** 写入排队原因：整组视角下第一个放不下的任务及其瓶颈维度。 */
    private void explainQueue(String groupId, List<Job> tasks, Job failedTask, boolean isArray) {
        ResourceRequest req = failedTask.request();
        String bottleneck = bottleneckDimension(req);
        String prefix = isArray
                ? "阵列 " + groupId + " 整组资源不足（" + tasks.size() + " 个任务需同时满足），整体排队"
                : "作业 " + groupId + " 资源暂不满足，排队等待";
        String reason = prefix + "；瓶颈维度=" + bottleneck + "，单任务请求=" + req.cores()
                + "核/" + req.memoryMb() + "MB/" + req.gpus() + "卡";
        for (Job job : tasks) {
            job.queueReason(reason);
        }
        log.info("排队: {}", reason);
    }

    /** 找出集群维度上最紧缺的资源维度（用于可解释排队原因）。 */
    private String bottleneckDimension(ResourceRequest req) {
        List<String> dims = new ArrayList<>();
        long freeCores = 0;
        long freeMem = 0;
        long freeGpus = 0;
        for (ComputeNode node : state.nodes().values()) {
            if (node.state() == NodeState.ONLINE) {
                freeCores += node.availableCores();
                freeMem += node.availableMemoryMb();
                freeGpus += node.availableGpus();
            }
        }
        if (req.cores() > freeCores) {
            dims.add("CPU(请求" + req.cores() + "/空闲" + freeCores + ")");
        }
        if (req.memoryMb() > freeMem) {
            dims.add("内存(请求" + req.memoryMb() + "/空闲" + freeMem + ")");
        }
        if (req.gpus() > freeGpus) {
            dims.add("加速卡(请求" + req.gpus() + "/空闲" + freeGpus + ")");
        }
        return dims.isEmpty() ? "整组分散放置不可行(碎片)" : String.join(",", dims);
    }

    // ============================ 确定性顺序 + 防饥饿 ============================

    private int compareGroups(List<Job> a, List<Job> b, long now) {
        Job ha = a.get(0);
        Job hb = b.get(0);
        int ea = effectivePriority(ha, now);
        int eb = effectivePriority(hb, now);
        if (ea != eb) {
            return Integer.compare(eb, ea); // 有效优先级高者先
        }
        int cmp = Long.compare(ha.submittedAtMillis(), hb.submittedAtMillis()); // 同优先级先到先得
        if (cmp != 0) {
            return cmp;
        }
        // 同一时刻提交：以全局提交序号为最终平局依据，保证结果确定且符合真实提交顺序
        return Long.compare(ha.submitSeq(), hb.submitSeq());
    }

    /** 有效优先级 = 静态优先级 + 老化提升；等待越久提升越多，封顶 maxBoost 防止饥饿。 */
    private int effectivePriority(Job job, long now) {
        long waited = now - job.submittedAtMillis();
        int boost = (int) Math.min(props.getMaxBoost(),
                waited / Math.max(1L, props.getStarvationBoostMillis()));
        return job.priority() + boost;
    }
}

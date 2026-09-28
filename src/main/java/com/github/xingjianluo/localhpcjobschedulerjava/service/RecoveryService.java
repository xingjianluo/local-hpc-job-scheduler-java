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
import com.github.xingjianluo.localhpcjobschedulerjava.domain.TerminationCause;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.WorkerAttempt;
import com.github.xingjianluo.localhpcjobschedulerjava.store.SchedulerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 进程重启后从本地事件日志重放，恢复内存状态。
 * 关键语义：
 * - 事件代表"已发生的事实"，重放时直接应用终态/释放效果，不再做重试决策；
 * - 崩溃发生在 ATTEMPT_STARTED 之后、完成/释放事件之前：该 attempt 成为孤儿，
 *   恢复时判定为丢失（节点视为在线但工作进程随重启消失），按失败策略收敛（重试/终止）；
 * - 事件序号天然幂等，重复重放同一批事件结果一致。
 */
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final SchedulerState state;
    private final TimeService time;
    private final SchedulerProperties props;

    public RecoveryService(SchedulerState state, TimeService time, SchedulerProperties props) {
        this.state = state;
        this.time = time;
        this.props = props;
    }

    /** 重放事件日志，恢复内存状态。返回重放的事件数。 */
    public int recover() {
        List<SchedulerEvent> events = state.eventLog().replay();
        replay(events);
        return events.size();
    }

    /** 按事件列表重放（不加锁：仅在启动单线程阶段或测试中调用）。 */
    void replay(List<SchedulerEvent> events) {
        synchronized (state.lock()) {
            for (SchedulerEvent e : events) {
                apply(e);
            }
            reconcileAfterReplay();
        }
    }

    private void apply(SchedulerEvent e) {
        switch (e.type()) {
            case "NODE_REGISTERED" -> applyNodeRegistered(e);
            case "NODE_LOST" -> applyNodeLost(e);
            case "JOB_SUBMITTED" -> applyJobSubmitted(e);
            case "ARRAY_SUBMITTED" -> applyArraySubmitted(e);
            case "ATTEMPT_STARTED" -> applyAttemptStarted(e);
            case "ATTEMPT_RELEASED" -> applyAttemptReleased(e);
            case "JOB_REQUEUED" -> applyJobRequeued(e);
            case "JOB_COMPLETED", "JOB_CANCELLED", "JOB_FAILED" -> applyJobTerminal(e);
            case "ARRAY_FAILED", "HEARTBEAT_TIMEOUT" -> {
                // 仅解释性记录：状态效果已由 JOB_* 事件体现，重放无需动作
            }
            default -> log.warn("重放遇到未知事件类型，忽略: seq={}, type={}", e.seq(), e.type());
        }
    }

    private void applyNodeRegistered(SchedulerEvent e) {
        List<String> p = e.payload();
        int cores = Integer.parseInt(p.get(0));
        int mem = Integer.parseInt(p.get(1));
        int gpus = Integer.parseInt(p.get(2));
        if (!state.nodes().containsKey(e.nodeId())) {
            state.nodes().put(e.nodeId(), new ComputeNode(e.nodeId(), cores, mem, gpus));
        }
    }

    private void applyNodeLost(SchedulerEvent e) {
        ComputeNode node = state.nodes().get(e.nodeId());
        if (node != null) {
            node.state(NodeState.LOST);
        }
    }

    private void applyJobSubmitted(SchedulerEvent e) {
        List<String> p = e.payload();
        if (state.jobs().containsKey(e.jobId())) {
            return;
        }
        String key = p.get(0);
        ResourceRequest req = new ResourceRequest(
                Integer.parseInt(p.get(1)), Integer.parseInt(p.get(2)),
                Integer.parseInt(p.get(3)), Integer.parseInt(p.get(4)));
        int prio = Integer.parseInt(p.get(5));
        int retries = Integer.parseInt(p.get(6));
        Job job = new Job(e.jobId(), null, -1, req, prio, retries, e.millis(),
                state.nextSubmitSeq());
        state.jobs().put(e.jobId(), job);
        state.dedupJobKeys().put(key, e.jobId());
    }

    private void applyArraySubmitted(SchedulerEvent e) {
        List<String> p = e.payload();
        String key = p.get(0);
        int taskCount = Integer.parseInt(p.get(1));
        ResourceRequest req = new ResourceRequest(
                Integer.parseInt(p.get(2)), Integer.parseInt(p.get(3)),
                Integer.parseInt(p.get(4)), Integer.parseInt(p.get(5)));
        int prio = Integer.parseInt(p.get(6));
        int retries = Integer.parseInt(p.get(7));
        JobArray array = new JobArray(e.jobId(), e.millis());
        for (int i = 0; i < taskCount; i++) {
            String taskId = e.jobId() + "-" + i;
            if (!state.jobs().containsKey(taskId)) {
                Job task = new Job(taskId, e.jobId(), i, req, prio, retries, e.millis(),
                        state.nextSubmitSeq());
                state.jobs().put(taskId, task);
                array.tasks().add(task);
            }
        }
        state.arrays().put(e.jobId(), array);
        state.dedupArrayKeys().put(key, e.jobId());
    }

    private void applyAttemptStarted(SchedulerEvent e) {
        Job job = state.jobs().get(e.jobId());
        if (job == null) {
            return;
        }
        List<String> p = e.payload();
        int attemptNo = Integer.parseInt(p.get(0));
        // 同一 attempt 已重放过（幂等）
        if (job.currentAttempt() != null
                && e.attemptId().equals(job.currentAttempt().attemptId())) {
            return;
        }
        WorkerAttempt attempt = new WorkerAttempt(e.attemptId(), attemptNo, e.nodeId(),
                e.millis(), e.millis());
        ComputeNode node = state.nodes().get(e.nodeId());
        if (node != null && node.state() == NodeState.ONLINE) {
            // 重放期 allocate 可能遇到容量不足（崩溃场景），保守记账：
            // 先直接登记，孤儿尝试在 reconcile 阶段统一判定丢失并释放
            node.allocate(e.attemptId(), job.id(), job.request());
        }
        state.attemptIndex().put(e.attemptId(), job.id());
        job.attemptCount(Math.max(job.attemptCount(), attemptNo));
        job.currentAttempt(attempt);
        job.state(JobState.RUNNING);
    }

    private void applyAttemptReleased(SchedulerEvent e) {
        Job job = state.jobs().get(e.jobId());
        ComputeNode node = e.nodeId() == null ? null : state.nodes().get(e.nodeId());
        if (node != null && node.runningAttempts().containsKey(e.attemptId())) {
            ResourceRequest req = job == null ? inferRequest(e) : job.request();
            if (req != null) {
                node.release(e.attemptId(), req);
            }
        }
        state.attemptIndex().remove(e.attemptId());
        if (job != null && job.currentAttempt() != null
                && e.attemptId().equals(job.currentAttempt().attemptId())) {
            job.currentAttempt().released(true);
            job.currentAttempt(null);
        }
    }

    private void applyJobRequeued(SchedulerEvent e) {
        Job job = state.jobs().get(e.jobId());
        if (job == null) {
            return;
        }
        if (job.currentAttempt() != null
                && e.attemptId() != null
                && e.attemptId().equals(job.currentAttempt().attemptId())) {
            job.currentAttempt(null);
        }
        job.state(JobState.PENDING);
        // 重放后立即可调度（退避仅对当次运行期有意义）
        job.readyAtMillis(0L);
        job.queueReason("重启恢复：等待重试");
    }

    private void applyJobTerminal(SchedulerEvent e) {
        Job job = state.jobs().get(e.jobId());
        if (job == null) {
            return;
        }
        // 终态事件幂等：已是终态则不覆盖（保留首个终态成因）
        if (job.isTerminal()) {
            return;
        }
        // 确保资源已释放（正常情况下此前已有 ATTEMPT_RELEASED，这里防御性兜底）
        if (job.currentAttempt() != null) {
            JobService.releaseCurrentAttempt(state, time, job);
        }
        switch (e.type()) {
            case "JOB_COMPLETED" -> {
                job.state(JobState.COMPLETED);
                job.terminationCause(TerminationCause.COMPLETED_BY_WORKER);
            }
            case "JOB_CANCELLED" -> {
                job.state(JobState.CANCELLED);
                job.terminationCause(TerminationCause.CANCELLED_BY_CLIENT);
            }
            case "JOB_FAILED" -> {
                job.state(JobState.FAILED);
                TerminationCause cause = e.payload().isEmpty()
                        ? TerminationCause.MAX_RETRIES_EXHAUSTED
                        : safeCause(e.payload().get(0));
                job.terminationCause(cause);
            }
            default -> { }
        }
        if (!e.payload().isEmpty()) {
            job.detail(e.payload().get(e.payload().size() - 1));
        }
    }

    private static TerminationCause safeCause(String name) {
        try {
            return TerminationCause.valueOf(name);
        } catch (IllegalArgumentException ex) {
            return TerminationCause.MAX_RETRIES_EXHAUSTED;
        }
    }

    private ResourceRequest inferRequest(SchedulerEvent e) {
        Job job = state.jobs().get(e.jobId());
        return job == null ? null : job.request();
    }

    /**
     * 重放后对账：
     * 1) 仍处 RUNNING 的 attempt 视为孤儿（进程已重启，工作进程不复存在）：
     *    节点在线则按失败策略重试/终止；节点已 LOST 同样收敛（释放防御性，资源可能已在节点侧清零）；
     * 2) 校验所有节点用量非负。
     */
    private void reconcileAfterReplay() {
        int orphan = 0;
        for (Job job : state.jobs().values()) {
            if (job.state() == JobState.RUNNING && job.currentAttempt() != null) {
                WorkerAttempt a = job.currentAttempt();
                orphan++;
                log.info("恢复发现孤儿尝试: job={}, attempt={}, node={}，按丢失收敛",
                        job.id(), a.attemptId(), a.nodeId());
                FailurePolicy.convergeLostAttempt(state, time, props, job,
                        a.attemptId(), a.nodeId(), "进程重启恢复：孤儿尝试判定丢失");
            }
        }
        // 重算集群容量
        int cores = 0, mem = 0, gpus = 0;
        for (ComputeNode node : state.nodes().values()) {
            if (node.usedCores() < 0 || node.usedMemoryMb() < 0 || node.usedGpus() < 0) {
                throw new IllegalStateException("恢复对账失败：节点 " + node.id() + " 用量为负");
            }
            cores += node.totalCores();
            mem += node.totalMemoryMb();
            gpus += node.totalGpus();
        }
        state.clusterCapacity(new ResourceRequest(cores, mem, gpus, Integer.MAX_VALUE));
        log.info("恢复对账完成: 节点={}, 作业={}, 阵列={}, 孤儿尝试收敛={}, 集群总容量={}核/{}MB/{}卡",
                state.nodes().size(), state.jobs().size(), state.arrays().size(),
                orphan, cores, mem, gpus);
    }
}

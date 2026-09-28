package com.github.xingjianluo.localhpcjobschedulerjava.service;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerException;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerState;
import com.github.xingjianluo.localhpcjobschedulerjava.core.TimeService;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ComputeNode;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobArray;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.RejectReason;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ResourceRequest;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.TerminationCause;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.WorkerAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 作业生命周期服务：提交校验、取消、心跳、完成。
 * 所有公共方法在全局锁内完成"检查 + 变更 + 事件落盘"，保证并发下一致。
 */
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final SchedulerState state;
    private final TimeService time;
    private final SchedulerProperties props;

    public JobService(SchedulerState state, TimeService time, SchedulerProperties props) {
        this.state = state;
        this.time = time;
        this.props = props;
    }

    // ============================ 提交 ============================

    /**
     * 提交单个作业。
     *
     * @param jobKey 客户端去重键（通常等于 clientJobId）；重复提交返回已存在作业且不重复占资源
     */
    public Job submitJob(String jobKey, String clientJobId, ResourceRequest request,
                         Integer priority, Integer maxRetries) {
        ResourceRequest validated = validate(request);
        int prio = priority == null ? 0 : priority;
        int retries = maxRetries == null ? props.getDefaultMaxRetries() : maxRetries;
        if (retries < 0) {
            throw new SchedulerException(RejectReason.INVALID_DURATION, "maxRetries 不能为负: " + retries);
        }
        String key = jobKey == null || jobKey.isBlank()
                ? "job:" + (clientJobId == null ? UUID.randomUUID().toString() : clientJobId)
                : "job:" + jobKey;

        synchronized (state.lock()) {
            String existingId = state.dedupJobKeys().get(key);
            if (existingId != null) {
                Job existing = state.jobs().get(existingId);
                log.info("重复提交被幂等拦截: key={} -> 已存在作业 {}, state={}, 不重复占用资源",
                        key, existingId, existing.state());
                throw new SchedulerException(RejectReason.DUPLICATE_SUBMISSION,
                        "作业重复提交，已存在 jobId=" + existingId + "，状态=" + existing.state()
                                + "；未重复分配任何资源");
            }
            checkClusterCapacity(validated, 1);
            String id = "job-" + UUID.randomUUID();
            Job job = new Job(id, null, -1, validated, prio, retries, time.nowMillis(),
                    state.nextSubmitSeq());
            state.jobs().put(id, job);
            state.dedupJobKeys().put(key, id);
            state.eventLog().append("JOB_SUBMITTED", time.nowMillis(), id, null, null,
                    List.of(key, String.valueOf(validated.cores()),
                            String.valueOf(validated.memoryMb()), String.valueOf(validated.gpus()),
                            String.valueOf(validated.durationLimitSec()),
                            String.valueOf(prio), String.valueOf(retries)));
            log.info("作业提交: id={}, request={}核/{}MB/{}卡/{}s, priority={}, maxRetries={}",
                    id, validated.cores(), validated.memoryMb(), validated.gpus(),
                    validated.durationLimitSec(), prio, retries);
            return job;
        }
    }

    /**
     * 提交作业阵列：校验 + 去重通过后，一次性创建全部任务（均为 PENDING）。
     * 资源不满足时不拒绝提交，而是整体留在队列（整组调度在 SchedulerService 中保证）。
     */
    public List<Job> submitArray(String arrayKey, String clientArrayId, int taskCount,
                                 ResourceRequest request, Integer priority, Integer maxRetries) {
        if (taskCount <= 0) {
            throw new SchedulerException(RejectReason.ZERO_TASKS, "阵列任务数必须为正: " + taskCount);
        }
        ResourceRequest validated = validate(request);
        int prio = priority == null ? 0 : priority;
        int retries = maxRetries == null ? props.getDefaultMaxRetries() : maxRetries;
        if (retries < 0) {
            throw new SchedulerException(RejectReason.INVALID_DURATION, "maxRetries 不能为负: " + retries);
        }
        String key = "array:" + (arrayKey == null || arrayKey.isBlank()
                ? (clientArrayId == null ? UUID.randomUUID().toString() : clientArrayId)
                : arrayKey);

        synchronized (state.lock()) {
            String existingArrayId = state.dedupArrayKeys().get(key);
            if (existingArrayId != null) {
                log.info("阵列重复提交被幂等拦截: key={} -> 已存在阵列 {}", key, existingArrayId);
                throw new SchedulerException(RejectReason.DUPLICATE_SUBMISSION,
                        "阵列重复提交，已存在 arrayId=" + existingArrayId + "；未重复分配任何资源");
            }
            // 阵列维度的容量判定：单任务超出任意节点 -> 永远无法整组调度
            checkArrayFeasibility(validated, taskCount);

            String arrayId = "array-" + UUID.randomUUID();
            JobArray array = new JobArray(arrayId, time.nowMillis());
            long now = time.nowMillis();
            List<Job> tasks = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                String jobId = arrayId + "-" + i;
                Job task = new Job(jobId, arrayId, i, validated, prio, retries, now,
                        state.nextSubmitSeq());
                state.jobs().put(jobId, task);
                array.tasks().add(task);
                tasks.add(task);
            }
            state.arrays().put(arrayId, array);
            state.dedupArrayKeys().put(key, arrayId);
            state.eventLog().append("ARRAY_SUBMITTED", now, arrayId, null, null,
                    List.of(key, String.valueOf(taskCount), String.valueOf(validated.cores()),
                            String.valueOf(validated.memoryMb()), String.valueOf(validated.gpus()),
                            String.valueOf(validated.durationLimitSec()),
                            String.valueOf(prio), String.valueOf(retries)));
            log.info("阵列提交: id={}, tasks={}, 单任务={}核/{}MB/{}卡, priority={}",
                    arrayId, taskCount, validated.cores(), validated.memoryMb(), validated.gpus(), prio);
            return tasks;
        }
    }

    // ============================ 查询 ============================

    public Job getJob(String jobId) {
        synchronized (state.lock()) {
            return requireJob(jobId);
        }
    }

    private Job requireJob(String jobId) {
        Job job = state.jobs().get(jobId);
        if (job == null) {
            throw new SchedulerException(RejectReason.JOB_NOT_FOUND, "作业不存在: " + jobId);
        }
        return job;
    }

    // ============================ 取消 ============================

    /**
     * 取消作业：
     * - PENDING：直接 CANCELLED（从未占资源）；
     * - RUNNING：释放当前 attempt 资源（仅一次）后 CANCELLED；
     * - 终态：幂等返回当前终态（取消与完成竞争时先到者生效，状态不倒退）。
     */
    public Job cancel(String jobId, String reason) {
        synchronized (state.lock()) {
            Job job = requireJob(jobId);
            if (job.isTerminal()) {
                log.info("取消已终态作业 {}(state={})：幂等返回，状态不变，不重复释放资源",
                        jobId, job.state());
                return job;
            }
            String detail = reason == null ? "client-cancel" : reason;
            if (job.state() == JobState.RUNNING && job.currentAttempt() != null) {
                releaseCurrentAttempt(state, time, job);
            }
            job.state(JobState.CANCELLED);
            job.terminationCause(TerminationCause.CANCELLED_BY_CLIENT);
            job.detail(detail);
            job.queueReason(null);
            state.eventLog().append("JOB_CANCELLED", time.nowMillis(), job.id(), null, null,
                    List.of(detail));
            log.info("作业取消终态: job={}, 原状态已收敛, detail={}", jobId, detail);
            return job;
        }
    }

    // ============================ 心跳 ============================

    /**
     * 心跳：仅 RUNNING 且 attemptId 匹配时刷新时间戳。
     * 重复/迟到（旧 attempt 或已终态）心跳一律幂等忽略，不改状态、不报错污染状态。
     */
    public void heartbeat(String jobId, String attemptId) {
        synchronized (state.lock()) {
            Job job = requireJob(jobId);
            WorkerAttempt cur = job.currentAttempt();
            if (job.state() != JobState.RUNNING || cur == null) {
                log.info("忽略心跳: job={} 不在运行中(state={})", jobId, job.state());
                return;
            }
            if (!cur.attemptId().equals(attemptId)) {
                log.info("忽略过期心跳: job={}, 上报attempt={}, 当前attempt={}",
                        jobId, attemptId, cur.attemptId());
                return;
            }
            long now = time.nowMillis();
            cur.lastHeartbeatMillis(now);
            log.debug("心跳刷新: job={}, attempt={}, lastHb={}", jobId, attemptId, now);
        }
    }

    // ============================ 完成 ============================

    /**
     * 完成上报：RUNNING 且 attempt 匹配 -> 释放资源一次并置 COMPLETED。
     * 重复完成上报（含同一 attempt 重发）幂等：保持 COMPLETED，不重复释放资源。
     * 旧 attempt 的迟到上报、已取消作业的上报均不得让状态倒退。
     */
    public Job complete(String jobId, String attemptId) {
        synchronized (state.lock()) {
            Job job = requireJob(jobId);
            if (job.state() == JobState.COMPLETED) {
                log.info("重复完成上报幂等忽略: job={} 已 COMPLETED，不重复释放资源", jobId);
                return job;
            }
            if (job.isTerminal()) {
                log.info("完成上报与终态 {} 竞争失败: job={}，保持唯一终态不变", job.state(), jobId);
                return job;
            }
            WorkerAttempt cur = job.currentAttempt();
            if (job.state() != JobState.RUNNING || cur == null) {
                log.info("完成上报时作业 {} 不在运行中(state={})，忽略", jobId, job.state());
                return job;
            }
            if (!cur.attemptId().equals(attemptId)) {
                log.info("忽略旧 attempt 的完成上报: job={}, 上报={}, 当前={}",
                        jobId, attemptId, cur.attemptId());
                return job;
            }
            releaseCurrentAttempt(state, time, job);
            job.state(JobState.COMPLETED);
            job.terminationCause(TerminationCause.COMPLETED_BY_WORKER);
            job.detail("worker-reported-complete");
            state.eventLog().append("JOB_COMPLETED", time.nowMillis(), job.id(),
                    cur.nodeId(), attemptId, List.of());
            log.info("作业完成终态: job={}, node={}, attempt={}", jobId, cur.nodeId(), attemptId);
            return job;
        }
    }

    // ============================ 校验（可区分原因） ============================

    static ResourceRequest validate(ResourceRequest r) {
        if (r == null) {
            throw new SchedulerException(RejectReason.INVALID_CORES, "资源请求为空");
        }
        if (r.cores() <= 0) {
            throw new SchedulerException(RejectReason.INVALID_CORES, "请求核数非法，必须>0: " + r.cores());
        }
        if (r.memoryMb() <= 0) {
            throw new SchedulerException(RejectReason.INVALID_MEMORY, "请求内存非法，必须>0(MB): " + r.memoryMb());
        }
        if (r.gpus() < 0) {
            throw new SchedulerException(RejectReason.INVALID_GPUS, "请求加速卡数量非法，必须>=0: " + r.gpus());
        }
        if (r.durationLimitSec() <= 0) {
            throw new SchedulerException(RejectReason.INVALID_DURATION,
                    "时长上限非法，必须>0(秒): " + r.durationLimitSec());
        }
        return r;
    }

    /** 单作业容量检查：超过所有在线节点容量之和则永久拒绝。调用方持锁。 */
    private void checkClusterCapacity(ResourceRequest r, int taskCount) {
        ResourceRequest cap = state.clusterCapacity();
        if (r.cores() * taskCount > cap.cores()
                || r.memoryMb() * taskCount > cap.memoryMb()
                || r.gpus() * taskCount > cap.gpus()) {
            throw new SchedulerException(RejectReason.EXCEEDS_CLUSTER_CAPACITY,
                    "请求超出集群总容量: 请求=" + r.cores() * taskCount + "核/"
                            + r.memoryMb() * taskCount + "MB/" + r.gpus() * taskCount
                            + "卡, 集群总容量=" + cap.cores() + "核/" + cap.memoryMb()
                            + "MB/" + cap.gpus() + "卡");
        }
    }

    /**
     * 阵列可行性检查（提交时即拒绝两类永不可调度请求）：
     * 1) 整组总需求超过集群总量 -> EXCEEDS_CLUSTER_CAPACITY；
     * 2) 单任务超过任一在线节点（无法放置）-> EXCEEDS_NODE_CAPACITY。
     */
    private void checkArrayFeasibility(ResourceRequest r, int taskCount) {
        checkClusterCapacity(r, taskCount);
        boolean fitsSomeNode = false;
        for (ComputeNode node : state.nodes().values()) {
            if (r.cores() <= node.totalCores()
                    && r.memoryMb() <= node.totalMemoryMb()
                    && r.gpus() <= node.totalGpus()) {
                fitsSomeNode = true;
                break;
            }
        }
        if (!fitsSomeNode) {
            throw new SchedulerException(RejectReason.EXCEEDS_NODE_CAPACITY,
                    "阵列单任务无法放入任何节点: 请求=" + r.cores() + "核/" + r.memoryMb()
                            + "MB/" + r.gpus() + "卡");
        }
    }

    /** 释放作业当前 attempt 的资源并清理索引（调用方持锁；幂等）。 */
    static void releaseCurrentAttempt(SchedulerState state, TimeService time, Job job) {
        WorkerAttempt cur = job.currentAttempt();
        if (cur == null) {
            return;
        }
        ComputeNode node = state.nodes().get(cur.nodeId());
        if (node != null) {
            node.release(cur.attemptId(), job.request());
        }
        state.attemptIndex().remove(cur.attemptId());
        cur.released(true);
        job.currentAttempt(null);
    }
}

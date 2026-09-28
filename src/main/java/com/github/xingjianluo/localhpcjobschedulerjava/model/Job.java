package com.github.xingjianluo.localhpcjobschedulerjava.model;

import java.util.List;

/**
 * 一个作业：单作业即 arraySize=1 的阵列。
 *
 * <p>调度以作业为单位整组进行：要么其全部任务一次性拿到资源，要么整体留在队列，
 * 绝不允许先占用一部分任务的资源再长期空等（gang scheduling）。</p>
 *
 * <p>运行期任一任务因节点失效/心跳超时失败时，按整组语义处理：同作业其余存活任务
 * 一并回收，整个作业进入 RETRY_WAIT，退避结束后整组重新调度。任意任务尝试次数用尽，
 * 整个作业 FAILED。</p>
 */
public final class Job {

    private final String id;
    private final String name;
    private final int arraySize;
    private final Resources perTaskRequest;
    private final long durationLimitMs;
    /** 用户指定静态优先级，数值越大越优先；老化补偿由引擎按等待时长叠加。 */
    private final int priority;
    /** 提交序号：同分时严格按提交先后，保证“同一时刻调度结果确定”。 */
    private final long submitSeq;
    private final long createdAtMs;
    /** 单个任务允许的最大重试次数（attemptNo-1 &lt;= maxRetries 才可再调度）。 */
    private final int maxRetries;
    /** 失败后重新进入调度的退避时长；0 表示立即可重试。 */
    private final long retryBackoffMs;

    private final List<Task> tasks;

    private JobState state;
    private QueueReason queueReason;
    private String reasonDetail;
    private Long retryEligibleAtMs;

    public Job(String id, String name, int arraySize, Resources perTaskRequest,
               long durationLimitMs, int priority, long submitSeq, long createdAtMs,
               int maxRetries, long retryBackoffMs, List<Task> tasks) {
        this(id, name, arraySize, perTaskRequest, durationLimitMs, priority, submitSeq,
                createdAtMs, maxRetries, retryBackoffMs, tasks, JobState.QUEUED,
                QueueReason.NEVER_SCHEDULED, "尚未进行调度", null);
    }

    /** 持久化恢复用全参构造器。 */
    @com.fasterxml.jackson.annotation.JsonCreator
    public Job(
            @com.fasterxml.jackson.annotation.JsonProperty("id") String id,
            @com.fasterxml.jackson.annotation.JsonProperty("name") String name,
            @com.fasterxml.jackson.annotation.JsonProperty("arraySize") int arraySize,
            @com.fasterxml.jackson.annotation.JsonProperty("perTaskRequest") Resources perTaskRequest,
            @com.fasterxml.jackson.annotation.JsonProperty("durationLimitMs") long durationLimitMs,
            @com.fasterxml.jackson.annotation.JsonProperty("priority") int priority,
            @com.fasterxml.jackson.annotation.JsonProperty("submitSeq") long submitSeq,
            @com.fasterxml.jackson.annotation.JsonProperty("createdAtMs") long createdAtMs,
            @com.fasterxml.jackson.annotation.JsonProperty("maxRetries") int maxRetries,
            @com.fasterxml.jackson.annotation.JsonProperty("retryBackoffMs") long retryBackoffMs,
            @com.fasterxml.jackson.annotation.JsonProperty("tasks") List<Task> tasks,
            @com.fasterxml.jackson.annotation.JsonProperty("state") JobState state,
            @com.fasterxml.jackson.annotation.JsonProperty("queueReason") QueueReason queueReason,
            @com.fasterxml.jackson.annotation.JsonProperty("reasonDetail") String reasonDetail,
            @com.fasterxml.jackson.annotation.JsonProperty("retryEligibleAtMs") Long retryEligibleAtMs) {
        this.id = id;
        this.name = name;
        this.arraySize = arraySize;
        this.perTaskRequest = perTaskRequest;
        this.durationLimitMs = durationLimitMs;
        this.priority = priority;
        this.submitSeq = submitSeq;
        this.createdAtMs = createdAtMs;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
        this.tasks = List.copyOf(tasks);
        this.state = state;
        this.queueReason = queueReason;
        this.reasonDetail = reasonDetail;
        this.retryEligibleAtMs = retryEligibleAtMs;
    }

    public String id() {
        return id;
    }

    public String name() {
        return name;
    }

    public int arraySize() {
        return arraySize;
    }

    public Resources perTaskRequest() {
        return perTaskRequest;
    }

    public long durationLimitMs() {
        return durationLimitMs;
    }

    public int priority() {
        return priority;
    }

    public long submitSeq() {
        return submitSeq;
    }

    public long createdAtMs() {
        return createdAtMs;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public long retryBackoffMs() {
        return retryBackoffMs;
    }

    public List<Task> tasks() {
        return tasks;
    }

    public JobState state() {
        return state;
    }

    public QueueReason queueReason() {
        return queueReason;
    }

    public String reasonDetail() {
        return reasonDetail;
    }

    public Long retryEligibleAtMs() {
        return retryEligibleAtMs;
    }

    public Resources totalRequest() {
        return perTaskRequest.times(arraySize);
    }

    public void noteQueued(QueueReason reason, String detail) {
        this.queueReason = reason;
        this.reasonDetail = detail;
        if (state != JobState.RETRY_WAIT) {
            this.state = JobState.QUEUED;
        }
    }

    /** 整组调度成功：由引擎在全部任务 markScheduled 后调用。 */
    public void markDispatched() {
        this.state = JobState.RUNNING;
        this.queueReason = null;
        this.reasonDetail = null;
        this.retryEligibleAtMs = null;
    }

    public void markRetryWait(long nowMs, String detail) {
        this.state = JobState.RETRY_WAIT;
        this.queueReason = QueueReason.RETRY_BACKOFF;
        this.reasonDetail = detail;
        this.retryEligibleAtMs = nowMs + retryBackoffMs;
    }

    public void markCancelled(String detail) {
        this.state = JobState.CANCELLED;
        this.reasonDetail = detail;
    }

    public void markFailed(String detail) {
        this.state = JobState.FAILED;
        this.reasonDetail = detail;
    }

    public void markCompleted() {
        this.state = JobState.COMPLETED;
        this.reasonDetail = null;
    }

    /** 退避已结束，可重新参与整组调度。 */
    public boolean retryEligible(long nowMs) {
        return state == JobState.RETRY_WAIT
                && retryEligibleAtMs != null
                && nowMs >= retryEligibleAtMs;
    }

    /** 退避结束后回到排队状态。 */
    public void activateAfterBackoff(long nowMs) {
        if (retryEligible(nowMs)) {
            this.state = JobState.QUEUED;
            this.queueReason = QueueReason.NEVER_SCHEDULED;
            this.retryEligibleAtMs = null;
        }
    }

    public boolean isTerminal() {
        return state == JobState.COMPLETED
                || state == JobState.CANCELLED
                || state == JobState.FAILED;
    }

    /**
     * 依据各任务槽状态推导整作业聚合状态。引擎在每次状态变更后调用。
     * 终态优先级：FAILED &gt; CANCELLED &gt; COMPLETED，保证并发取消与完成竞争时收敛唯一。
     */
    public JobState deriveState() {
        boolean allCompleted = true;
        boolean allCancelledOrDone = true;
        for (Task t : tasks) {
            if (t.state() != JobState.COMPLETED) {
                allCompleted = false;
            }
            if (t.state() != JobState.COMPLETED && t.state() != JobState.CANCELLED) {
                allCancelledOrDone = false;
            }
            if (t.state() == JobState.FAILED) {
                return JobState.FAILED;
            }
        }
        if (allCompleted) {
            return JobState.COMPLETED;
        }
        if (allCancelledOrDone) {
            return JobState.CANCELLED;
        }
        boolean anyRunning = false;
        boolean anyRetryWait = false;
        boolean allQueued = true;
        for (Task t : tasks) {
            if (t.state() == JobState.RUNNING) {
                anyRunning = true;
                allQueued = false;
            } else if (t.state() == JobState.RETRY_WAIT) {
                anyRetryWait = true;
                allQueued = false;
            } else if (t.state() != JobState.QUEUED
                    && t.state() != JobState.COMPLETED
                    && t.state() != JobState.CANCELLED) {
                allQueued = false;
            }
        }
        if (anyRetryWait) {
            return JobState.RETRY_WAIT;
        }
        if (anyRunning) {
            return JobState.RUNNING;
        }
        if (allQueued) {
            return JobState.QUEUED;
        }
        return state;
    }

    /** 根据任务槽状态重新收敛作业状态（取消/完成竞争后调用）。 */
    public void reconcileFromTasks() {
        this.state = deriveState();
    }
}

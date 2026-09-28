package com.github.xingjianluo.localhpcjobschedulerjava.model;

/**
 * 作业阵列中的单个任务槽。单作业即 arraySize=1、只有一个 Task 的作业。
 *
 * <p>每个任务槽在任意时刻至多绑定一个运行中的工作进程：{@code attemptNo} 即尝试次数
 * （首次=1，每次重试加 1）。节点失效或心跳超时导致的重试不会重复计入资源用量，
 * 因为节点失联时其用量已被清零，重新调度走的是全新分配。</p>
 *
 * <p>终态（COMPLETED/CANCELLED/FAILED）不可逆；重复完成上报、重复心跳都通过
 * 状态判定被幂等忽略，因此资源不会被重复释放。</p>
 */
public final class Task {

    private final String id;
    private final int index;
    private final Resources request;
    private final long durationLimitMs;

    private JobState state;
    private int attemptNo;
    private String nodeId;
    private Long startedAtMs;
    private long lastHeartbeatMs;
    private String failReason;

    public Task(String id, int index, Resources request, long durationLimitMs) {
        this(id, index, request, durationLimitMs, JobState.QUEUED, 0, null, null, 0L, null);
    }

    /** 持久化恢复用全参构造器。 */
    @com.fasterxml.jackson.annotation.JsonCreator
    public Task(
            @com.fasterxml.jackson.annotation.JsonProperty("id") String id,
            @com.fasterxml.jackson.annotation.JsonProperty("index") int index,
            @com.fasterxml.jackson.annotation.JsonProperty("request") Resources request,
            @com.fasterxml.jackson.annotation.JsonProperty("durationLimitMs") long durationLimitMs,
            @com.fasterxml.jackson.annotation.JsonProperty("state") JobState state,
            @com.fasterxml.jackson.annotation.JsonProperty("attemptNo") int attemptNo,
            @com.fasterxml.jackson.annotation.JsonProperty("nodeId") String nodeId,
            @com.fasterxml.jackson.annotation.JsonProperty("startedAtMs") Long startedAtMs,
            @com.fasterxml.jackson.annotation.JsonProperty("lastHeartbeatMs") long lastHeartbeatMs,
            @com.fasterxml.jackson.annotation.JsonProperty("failReason") String failReason) {
        this.id = id;
        this.index = index;
        this.request = request;
        this.durationLimitMs = durationLimitMs;
        this.state = state;
        this.attemptNo = attemptNo;
        this.nodeId = nodeId;
        this.startedAtMs = startedAtMs;
        this.lastHeartbeatMs = lastHeartbeatMs;
        this.failReason = failReason;
    }

    public String id() {
        return id;
    }

    public int index() {
        return index;
    }

    public Resources request() {
        return request;
    }

    public long durationLimitMs() {
        return durationLimitMs;
    }

    public JobState state() {
        return state;
    }

    public int attemptNo() {
        return attemptNo;
    }

    public String nodeId() {
        return nodeId;
    }

    public Long startedAtMs() {
        return startedAtMs;
    }

    public long lastHeartbeatMs() {
        return lastHeartbeatMs;
    }

    public String failReason() {
        return failReason;
    }

    public boolean isTerminal() {
        return state == JobState.COMPLETED
                || state == JobState.CANCELLED
                || state == JobState.FAILED;
    }

    /** 是否已被分配到某个节点运行（RUNNING 或正在运行但尚未被检测的状态）。 */
    public boolean isRunning() {
        return state == JobState.RUNNING;
    }

    /**
     * 将任务槽调度到某节点：开启新一次尝试。
     * 由引擎在完成整组放置后调用，此处只做记录。
     */
    public void markScheduled(String nodeId, long nowMs) {
        this.state = JobState.RUNNING;
        this.attemptNo++;
        this.nodeId = nodeId;
        this.startedAtMs = nowMs;
        this.lastHeartbeatMs = nowMs;
        this.failReason = null;
    }

    /** 回到队列等待重新调度（重试退避结束后），释放旧绑定。 */
    public void backToQueue(String reason) {
        this.state = JobState.QUEUED;
        this.nodeId = null;
        this.startedAtMs = null;
        this.lastHeartbeatMs = 0L;
        this.failReason = reason;
    }

    /** 进入失败重试等待状态，记录原因并解除旧节点绑定（不增加 attemptNo，下次重新调度时才增加）。 */
    public void markRetryWait(String reason) {
        this.state = JobState.RETRY_WAIT;
        this.failReason = reason;
        this.nodeId = null;
        this.startedAtMs = null;
        this.lastHeartbeatMs = 0L;
    }

    public void heartbeat(long nowMs) {
        if (state == JobState.RUNNING) {
            this.lastHeartbeatMs = nowMs;
        }
    }

    public void markCompleted() {
        this.state = JobState.COMPLETED;
    }

    public void markCancelled() {
        this.state = JobState.CANCELLED;
    }

    public void markFailed(String reason) {
        this.state = JobState.FAILED;
        this.failReason = reason;
    }

    /** 进程重启恢复：曾经在运行的任务槽回到队列重新调度，不增加失败计数。 */
    public void recoverToQueue() {
        backToQueue("进程重启恢复，重新排队");
    }
}

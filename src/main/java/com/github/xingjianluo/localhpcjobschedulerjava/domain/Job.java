package com.github.xingjianluo.localhpcjobschedulerjava.domain;

/**
 * 单个作业（阵列时为其中一个任务）。
 */
public class Job {

    private final String id;
    private final String arrayId; // 非阵列时为 null
    private final int arrayIndex; // 非阵列时为 -1
    private final ResourceRequest request;
    private final int priority;
    private final int maxRetries;
    private final long submittedAtMillis;
    /** 全局单调提交序号：同优先级、同提交时刻的最终平局依据（真实先到先得，与随机 id 无关）。 */
    private final long submitSeq;

    private volatile JobState state = JobState.PENDING;
    private volatile String queueReason;
    private volatile WorkerAttempt currentAttempt;
    private volatile int attemptCount;
    private volatile long readyAtMillis; // 重新可调度时间（重试退避）
    private volatile TerminationCause terminationCause;
    private volatile String detail = "";

    public Job(String id, String arrayId, int arrayIndex, ResourceRequest request,
               int priority, int maxRetries, long submittedAtMillis, long submitSeq) {
        this.id = id;
        this.arrayId = arrayId;
        this.arrayIndex = arrayIndex;
        this.request = request;
        this.priority = priority;
        this.maxRetries = maxRetries;
        this.submittedAtMillis = submittedAtMillis;
        this.submitSeq = submitSeq;
        this.readyAtMillis = submittedAtMillis;
    }

    public long submitSeq() {
        return submitSeq;
    }

    public String id() { return id; }
    public String arrayId() { return arrayId; }
    public int arrayIndex() { return arrayIndex; }
    public ResourceRequest request() { return request; }
    public int priority() { return priority; }
    public int maxRetries() { return maxRetries; }
    public long submittedAtMillis() { return submittedAtMillis; }
    public JobState state() { return state; }
    public void state(JobState state) { this.state = state; }
    public String queueReason() { return queueReason; }
    public void queueReason(String queueReason) { this.queueReason = queueReason; }
    public WorkerAttempt currentAttempt() { return currentAttempt; }
    public void currentAttempt(WorkerAttempt attempt) { this.currentAttempt = attempt; }
    public int attemptCount() { return attemptCount; }
    public void attemptCount(int attemptCount) { this.attemptCount = attemptCount; }
    public long readyAtMillis() { return readyAtMillis; }
    public void readyAtMillis(long readyAtMillis) { this.readyAtMillis = readyAtMillis; }
    public TerminationCause terminationCause() { return terminationCause; }
    public void terminationCause(TerminationCause cause) { this.terminationCause = cause; }
    public String detail() { return detail; }
    public void detail(String detail) { this.detail = detail == null ? "" : detail; }

    public boolean isTerminal() {
        return state == JobState.COMPLETED || state == JobState.FAILED || state == JobState.CANCELLED;
    }
}

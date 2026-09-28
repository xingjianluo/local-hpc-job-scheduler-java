package com.github.xingjianluo.localhpcjobschedulerjava.domain;

/**
 * 一次工作进程运行尝试。重试会产生新的 attempt；
 * 资源用量按 attempt 精确记账，丢失的 attempt 在收敛时释放，重试不重复计资源。
 */
public class WorkerAttempt {

    private final String attemptId;
    private final int attemptNo;
    private final String nodeId;
    private final long startedAtMillis;
    private volatile long lastHeartbeatMillis;
    private volatile boolean released;

    public WorkerAttempt(String attemptId, int attemptNo, String nodeId,
                         long startedAtMillis, long lastHeartbeatMillis) {
        this.attemptId = attemptId;
        this.attemptNo = attemptNo;
        this.nodeId = nodeId;
        this.startedAtMillis = startedAtMillis;
        this.lastHeartbeatMillis = lastHeartbeatMillis;
    }

    public String attemptId() {
        return attemptId;
    }

    public int attemptNo() {
        return attemptNo;
    }

    public String nodeId() {
        return nodeId;
    }

    public long startedAtMillis() {
        return startedAtMillis;
    }

    public long lastHeartbeatMillis() {
        return lastHeartbeatMillis;
    }

    public void lastHeartbeatMillis(long t) {
        this.lastHeartbeatMillis = t;
    }

    public boolean released() {
        return released;
    }

    public void released(boolean released) {
        this.released = released;
    }
}

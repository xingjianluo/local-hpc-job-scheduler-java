package com.github.xingjianluo.localhpcjobschedulerjava.model;

/**
 * 模拟集群节点：持有固定容量与当前已分配用量，并维护在线/失联状态。
 *
 * <p>本对象自身不加锁；所有调用由 {@code SchedulerEngine} 的全局锁串行化，
 * 从而保证“检查可用量—分配”这一复合动作在并发调度下不会重复或超额分配。</p>
 *
 * <p>节点被置为 LOST/DOWN 时，其上绑定的任务由引擎统一失败转移（重试或终止），
 * 节点已用量同时清零——因为那些任务已不再存活，用量清零不属于“释放”，
 * 不会对同一个任务执行第二次资源归还。</p>
 */
public final class ClusterNode {

    private final String nodeId;
    private final Resources capacity;

    private Resources used;
    private NodeState state;
    private long lastHeartbeatMs;

    public ClusterNode(String nodeId, Resources capacity) {
        this(nodeId, capacity, Resources.ZERO, NodeState.UP, 0L);
    }

    /** 持久化恢复用全参构造器。 */
    @com.fasterxml.jackson.annotation.JsonCreator
    public ClusterNode(@com.fasterxml.jackson.annotation.JsonProperty("nodeId") String nodeId,
                       @com.fasterxml.jackson.annotation.JsonProperty("capacity") Resources capacity,
                       @com.fasterxml.jackson.annotation.JsonProperty("used") Resources used,
                       @com.fasterxml.jackson.annotation.JsonProperty("state") NodeState state,
                       @com.fasterxml.jackson.annotation.JsonProperty("lastHeartbeatMs") long lastHeartbeatMs) {
        this.nodeId = nodeId;
        this.capacity = capacity;
        this.used = used;
        this.state = state;
        this.lastHeartbeatMs = lastHeartbeatMs;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("nodeId")
    public String nodeId() {
        return nodeId;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("capacity")
    public Resources capacity() {
        return capacity;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("used")
    public Resources used() {
        return used;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("state")
    public NodeState state() {
        return state;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("lastHeartbeatMs")
    public long lastHeartbeatMs() {
        return lastHeartbeatMs;
    }

    @com.fasterxml.jackson.annotation.JsonIgnore
    public boolean isOnline() {
        return state == NodeState.UP;
    }

    @com.fasterxml.jackson.annotation.JsonIgnore
    public Resources available() {
        return capacity.minus(used);
    }

    /** 请求是否能整块放入该节点当前剩余容量。 */
    public boolean canFit(Resources request) {
        return isOnline() && request.fitsIn(available());
    }

    /**
     * 在节点上分配资源。调用方必须已经通过 {@link #canFit} 检查并持有全局锁。
     *
     * @throws IllegalStateException 若节点不在线或容量不足（防御性检查，正常流程不会触发）
     */
    public void allocate(Resources request) {
        if (!canFit(request)) {
            throw new IllegalStateException(
                    "节点 " + nodeId + " 无法满足分配 " + request
                            + "，当前状态=" + state + "，已用=" + used
                            + "，容量=" + capacity);
        }
        used = used.plus(request);
    }

    /** 归还资源。任务正常完成或被取消时调用；每个任务至多归还一次，由引擎状态机保证。 */
    public void release(Resources request) {
        Resources next = used.minus(request);
        if (next.hasNegativeDimension()) {
            // 双重释放的最后防线：并发状态机正确时不会触发；触发即说明状态流转有 bug。
            throw new IllegalStateException(
                    "节点 " + nodeId + " 检测到重复/超额释放 " + request
                            + "：当前已用 " + used + "，释放后将为负 " + next);
        }
        used = next;
    }

    /**
     * 将节点标记为失联/下线，返回被清空的已用量（仅用于日志解释）。
     * 绑定任务由引擎另行失败转移；此处清零不与任务级释放重复计数。
     */
    public Resources markNotOnline(NodeState newState) {
        if (newState == NodeState.UP) {
            throw new IllegalArgumentException("markNotOnline 不能把节点置为 UP");
        }
        Resources orphaned = used;
        used = Resources.ZERO;
        state = newState;
        return orphaned;
    }

    /** 节点恢复（或重新注册）：回到在线状态；其历史用量已在失联时清零。 */
    public void recover(long nowMs) {
        state = NodeState.UP;
        used = Resources.ZERO;
        lastHeartbeatMs = nowMs;
    }

    /** 工作进程心跳：仅在线节点接受，刷新最后心跳时间。 */
    public void heartbeat(long nowMs) {
        if (isOnline()) {
            lastHeartbeatMs = nowMs;
        }
    }
}

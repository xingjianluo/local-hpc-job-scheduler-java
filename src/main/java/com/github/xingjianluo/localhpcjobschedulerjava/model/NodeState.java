package com.github.xingjianluo.localhpcjobschedulerjava.model;

/**
 * 模拟节点的生命周期状态。
 */
public enum NodeState {
    /** 在线，可参与调度。 */
    UP,
    /** 心跳超时 / 被管理员置为失联，其上运行的任务需要重试或终止。 */
    LOST,
    /** 管理员主动下线，不可再调度；其上任务同样按失败策略处理。 */
    DOWN
}

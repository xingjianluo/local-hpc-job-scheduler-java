package com.github.xingjianluo.localhpcjobschedulerjava.model;

/**
 * 作业留在队列（本轮无法整组调度）的可解释原因。
 */
public enum QueueReason {
    /** 尚未进行过调度。 */
    NEVER_SCHEDULED,
    /** 整组需求一次性无法满足（散落后空闲资源总和够但无法整组放置）。 */
    INSUFFICIENT_FOR_GANG,
    /** 集群当前没有任何在线节点。 */
    NO_ONLINE_NODE,
    /** 优先级更高（含老化补偿）的作业排在前面，本作业本轮未被评估。 */
    WAITING_HIGHER_PRIORITY,
    /** 失败重试的退避等待尚未结束。 */
    RETRY_BACKOFF
}

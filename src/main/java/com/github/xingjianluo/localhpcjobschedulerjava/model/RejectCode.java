package com.github.xingjianluo.localhpcjobschedulerjava.model;

/**
 * 提交被拒绝时的可区分原因（校验类，作业不会入队）。
 */
public enum RejectCode {
    /** 作业名缺失。 */
    NAME_MISSING,
    /** 阵列任务数非法（小于 1）。 */
    INVALID_ARRAY_SIZE,
    /** 某资源维度为负数。 */
    NEGATIVE_RESOURCE,
    /** 运行时长上限非法（小于等于 0）。 */
    INVALID_DURATION_LIMIT,
    /** 单核内存等数值非法（保留，当前规则不使用）。 */
    INVALID_SPEC,
    /** 单个任务的请求超出集群中任何一个节点的容量，永远无法满足。 */
    EXCEEDS_MAX_NODE,
    /** 阵列总请求超出集群总容量，整组永远无法满足。 */
    EXCEEDS_CLUSTER_CAPACITY
}

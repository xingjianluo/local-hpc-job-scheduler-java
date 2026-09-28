package com.github.xingjianluo.localhpcjobschedulerjava.domain;

/**
 * 可区分的拒绝/排队原因。
 */
public enum RejectReason {
    INVALID_CORES("请求核数非法"),
    INVALID_MEMORY("请求内存非法"),
    INVALID_GPUS("请求加速卡数量非法"),
    INVALID_DURATION("时长上限非法"),
    ZERO_TASKS("阵列任务数为零"),
    INVALID_ARRAY_INDEX("阵列任务下标非法"),
    EXCEEDS_CLUSTER_CAPACITY("请求超出集群总容量"),
    EXCEEDS_NODE_CAPACITY("单个任务超出任意节点容量"),
    DUPLICATE_SUBMISSION("重复提交"),
    JOB_NOT_FOUND("作业不存在"),
    ILLEGAL_STATE("当前状态不允许该操作");

    private final String description;

    RejectReason(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}

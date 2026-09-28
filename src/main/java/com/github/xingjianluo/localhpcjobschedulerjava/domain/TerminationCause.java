package com.github.xingjianluo.localhpcjobschedulerjava.domain;

/**
 * 作业终态成因（可解释性）。
 */
public enum TerminationCause {
    COMPLETED_BY_WORKER("工作进程上报完成"),
    CANCELLED_BY_CLIENT("客户端取消"),
    CANCELLED_WHILE_LOSS("取消与节点/心跳丢失竞争，收敛为取消"),
    MAX_RETRIES_EXHAUSTED("重试次数耗尽"),
    ARRAY_TASK_FAILED("阵列中存在不可重试失败的任务");

    private final String description;

    TerminationCause(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}

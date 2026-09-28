package com.github.xingjianluo.localhpcjobschedulerjava.model;

/**
 * 作业（及其阵列任务）在整个生命周期中的状态。
 */
public enum JobState {
    /** 已通过校验并进入队列，等待资源。 */
    QUEUED,
    /** 所有任务已拿到资源、工作进程已启动。 */
    RUNNING,
    /** 运行中丢失工作进程，正在等待重试重新调度。 */
    RETRY_WAIT,
    /** 被用户取消，终态。 */
    CANCELLED,
    /** 全部任务成功完成，终态。 */
    COMPLETED,
    /** 重试次数用尽或被判定为不可恢复，终态。 */
    FAILED,
    /** 提交时校验未通过（仅用于提交拒绝响应，不会持久化为作业状态）。 */
    REJECTED
}

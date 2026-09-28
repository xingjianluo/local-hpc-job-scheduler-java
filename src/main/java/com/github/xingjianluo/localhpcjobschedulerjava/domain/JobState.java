package com.github.xingjianluo.localhpcjobschedulerjava.domain;

/**
 * 作业生命周期状态。
 * PENDING -> RUNNING -> COMPLETED / FAILED / CANCELLED
 */
public enum JobState {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
}

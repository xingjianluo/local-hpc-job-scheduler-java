package com.github.xingjianluo.localhpcjobschedulerjava.exception;

/**
 * 与当前状态冲突的操作，例如：
 * 对已终态作业取消、完成上报与取消竞争落败、心跳来自未运行任务等。
 * 冲突不会改变任何已有状态（失败不污染状态）。
 */
public class ConflictException extends RuntimeException {
    public ConflictException(String message) {
        super(message);
    }
}

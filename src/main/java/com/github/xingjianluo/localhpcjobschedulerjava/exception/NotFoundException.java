package com.github.xingjianluo.localhpcjobschedulerjava.exception;

/** 引用的作业/任务/节点不存在。 */
public class NotFoundException extends RuntimeException {
    public NotFoundException(String message) {
        super(message);
    }
}

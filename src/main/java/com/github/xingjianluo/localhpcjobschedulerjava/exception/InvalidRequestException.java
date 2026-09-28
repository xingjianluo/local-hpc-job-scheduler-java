package com.github.xingjianluo.localhpcjobschedulerjava.exception;

/** 请求格式本身非法（缺少字段、JSON 无法绑定等），与业务校验拒绝相区分。 */
public class InvalidRequestException extends RuntimeException {
    public InvalidRequestException(String message) {
        super(message);
    }
}

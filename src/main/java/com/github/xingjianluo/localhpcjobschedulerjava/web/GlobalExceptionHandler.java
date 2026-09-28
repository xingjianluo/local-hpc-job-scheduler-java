package com.github.xingjianluo.localhpcjobschedulerjava.web;

import com.github.xingjianluo.localhpcjobschedulerjava.exception.ConflictException;
import com.github.xingjianluo.localhpcjobschedulerjava.exception.InvalidRequestException;
import com.github.xingjianluo.localhpcjobschedulerjava.exception.JobRejectedException;
import com.github.xingjianluo.localhpcjobschedulerjava.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 把领域异常归类为可区分的 HTTP 错误：
 * 422=提交被业务规则拒绝（带 RejectCode）；409=状态冲突；404=不存在；400=请求格式非法。
 * 所有错误均不改变引擎内已有状态。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(JobRejectedException.class)
    public ResponseEntity<Map<String, Object>> rejected(JobRejectedException e) {
        log.warn("作业提交被拒绝 code={} message={}", e.code(), e.getMessage());
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of(
                "error", "JOB_REJECTED",
                "rejectCode", e.code().name(),
                "message", e.getMessage()));
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<Map<String, Object>> conflict(ConflictException e) {
        log.warn("状态冲突：{}", e.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "error", "STATE_CONFLICT",
                "message", e.getMessage()));
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<Map<String, Object>> notFound(NotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                "error", "NOT_FOUND",
                "message", e.getMessage()));
    }

    @ExceptionHandler({InvalidRequestException.class, IllegalArgumentException.class})
    public ResponseEntity<Map<String, Object>> invalid(Exception e) {
        log.warn("非法请求：{}", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                "error", "INVALID_REQUEST",
                "message", e.getMessage()));
    }
}

package com.github.xingjianluo.localhpcjobschedulerjava.web;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerException;
import com.github.xingjianluo.localhpcjobschedulerjava.web.dto.Dtos;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 业务异常 -> 422（可区分原因），其它异常 -> 400。
 */
@RestControllerAdvice
public class SchedulerExceptionHandler {

    @ExceptionHandler(SchedulerException.class)
    public ResponseEntity<Dtos.ErrorResponse> handleScheduler(SchedulerException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new Dtos.ErrorResponse("scheduler_error", ex.reason().name(), ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Dtos.ErrorResponse> handleOther(Exception ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new Dtos.ErrorResponse("bad_request", "BAD_REQUEST", ex.getMessage()));
    }
}

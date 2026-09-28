package com.github.xingjianluo.localhpcjobschedulerjava.web.dto;

import java.util.List;

/**
 * Web 层请求/响应 DTO 集合。
 */
public final class Dtos {

    private Dtos() {
    }

    public record NodeRequest(String nodeId, Integer cores, Integer memoryMb, Integer gpus) {
    }

    public record SubmitJobRequest(String clientId, Integer cores, Integer memoryMb, Integer gpus,
                                   Integer durationLimitSec, Integer priority, Integer maxRetries) {
    }

    public record SubmitArrayRequest(String clientId, Integer taskCount, Integer cores,
                                     Integer memoryMb, Integer gpus, Integer durationLimitSec,
                                     Integer priority, Integer maxRetries) {
    }

    public record CancelRequest(String reason) {
    }

    public record HeartbeatRequest(String attemptId) {
    }

    public record CompleteRequest(String attemptId) {
    }

    public record NodeView(String nodeId, String state, int totalCores, int usedCores,
                           int totalMemoryMb, int usedMemoryMb, int totalGpus, int usedGpus,
                           int runningAttempts) {
    }

    public record JobView(String jobId, String arrayId, int arrayIndex, String state,
                          int priority, int cores, int memoryMb, int gpus, int durationLimitSec,
                          int attemptCount, Integer maxRetries, String nodeId, String attemptId,
                          String queueReason, String terminationCause, String detail) {
    }

    public record SubmitResponse(String jobId, String arrayId, List<String> jobIds, String state) {
    }

    public record TickResponse(int started, long atMillis) {
    }

    public record ErrorResponse(String error, String reason, String message) {
    }
}

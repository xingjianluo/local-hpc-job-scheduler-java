package com.github.xingjianluo.localhpcjobschedulerjava.web;

import com.github.xingjianluo.localhpcjobschedulerjava.model.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.NodeState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.QueueReason;
import com.github.xingjianluo.localhpcjobschedulerjava.model.RejectCode;

import java.util.List;

/**
 * HTTP 层使用的数据传输对象。领域模型不直接暴露给序列化层，
 * 避免内部可变字段被外部请求写回。
 */
public final class Dtos {

    private Dtos() {
    }

    /** 提交单作业或作业阵列的请求。 */
    public record SubmitJobRequest(
            String jobId,
            String name,
            Integer arraySize,
            Long cpus,
            Long memoryMb,
            Long gpus,
            Long durationLimitMs,
            Integer priority,
            Integer maxRetries,
            Long retryBackoffMs) {
    }

    /**
     * 提交结果：accepted=true 时携带作业视图；
     * accepted=false 时 code/message 给出可区分的拒绝原因。
     * duplicate=true 表示幂等命中：同一 jobId 已存在，未重复占用资源。
     */
    public record SubmitResult(
            boolean accepted,
            boolean duplicate,
            String jobId,
            RejectCode rejectCode,
            String message,
            JobView job) {

        public static SubmitResult accepted(JobView job) {
            return new SubmitResult(true, false, job.jobId(), null, "已受理", job);
        }

        public static SubmitResult duplicate(JobView existing) {
            return new SubmitResult(true, true, existing.jobId(), null,
                    "作业已存在，重复提交已幂等忽略", existing);
        }

        public static SubmitResult rejected(String jobId, RejectCode code, String message) {
            return new SubmitResult(false, false, jobId, code, message, null);
        }
    }

    /** 任务槽视图。 */
    public record TaskView(
            String taskId,
            int index,
            JobState state,
            int attemptNo,
            String nodeId,
            Long startedAtMs,
            long lastHeartbeatMs,
            String failReason) {
    }

    /** 作业视图，含队列原因解释。 */
    public record JobView(
            String jobId,
            String name,
            int arraySize,
            long cpusPerTask,
            long memoryMbPerTask,
            long gpusPerTask,
            long durationLimitMs,
            int priority,
            JobState state,
            QueueReason queueReason,
            String reasonDetail,
            Long retryEligibleAtMs,
            List<TaskView> tasks) {
    }

    /** 节点视图。 */
    public record NodeView(
            String nodeId,
            NodeState state,
            long cpusCapacity,
            long memoryMbCapacity,
            long gpusCapacity,
            long cpusUsed,
            long memoryMbUsed,
            long gpusUsed,
            long lastHeartbeatMs) {
    }

    /** 集群总览。 */
    public record SnapshotView(
            long nowMs,
            List<NodeView> nodes,
            List<JobView> jobs) {
    }

    /** 通用操作结果。 */
    public record ActionResult(boolean ok, String action, String detail) {
        public static ActionResult of(String action, String detail) {
            return new ActionResult(true, action, detail);
        }
    }
}

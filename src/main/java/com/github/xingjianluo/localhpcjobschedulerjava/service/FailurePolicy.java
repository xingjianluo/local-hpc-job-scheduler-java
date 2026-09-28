package com.github.xingjianluo.localhpcjobschedulerjava.service;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerState;
import com.github.xingjianluo.localhpcjobschedulerjava.core.TimeService;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ComputeNode;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobArray;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.TerminationCause;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.WorkerAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 异常收敛策略（调用方必须持有全局锁）：
 * 1) attempt 丢失（节点失效或心跳超时）时，精确释放该 attempt 的资源（仅一次）；
 * 2) 未超出 maxRetries：作业回到 PENDING 等待重新调度，重试不额外占用资源；
 * 3) 已达上限：作业终止为 FAILED；阵列任务失败导致阵列整体标记失败；
 * 4) 若作业已被取消（终态竞争），保持取消终态，仅做资源清理，状态不倒退。
 */
final class FailurePolicy {

    private static final Logger log = LoggerFactory.getLogger(FailurePolicy.class);

    private FailurePolicy() {
    }

    /**
     * 收敛一个已丢失的 attempt。
     *
     * @param lostAttemptId 被判定丢失的尝试 id（可能与当前 attempt 不一致，用于幂等处理旧上报）
     */
    static void convergeLostAttempt(SchedulerState state, TimeService time,
                                    SchedulerProperties props, Job job,
                                    String lostAttemptId, String nodeId, String reason) {
        WorkerAttempt current = job.currentAttempt();

        // 1. 精确释放丢失 attempt 的节点资源（release 自身幂等，重复收敛不会重复释放）
        ComputeNode node = state.nodes().get(nodeId);
        if (node != null && current != null && current.attemptId().equals(lostAttemptId)) {
            node.release(lostAttemptId, job.request());
            state.attemptIndex().remove(lostAttemptId);
            current.released(true);
            log.info("释放丢失尝试资源: job={}, attempt={}, node={}, 资源={}核/{}MB/{}卡",
                    job.id(), lostAttemptId, nodeId, job.request().cores(),
                    job.request().memoryMb(), job.request().gpus());
        } else {
            // 节点已不存在或 attempt 已被其它路径释放：仅清理索引
            state.attemptIndex().remove(lostAttemptId);
            log.info("丢失尝试 {} 无需释放资源（node={}, job={}）", lostAttemptId, nodeId, job.id());
        }
        state.eventLog().append("ATTEMPT_RELEASED", time.nowMillis(), job.id(), nodeId,
                lostAttemptId, List.of(reason));

        // 2. 终态竞争：作业已经 CANCELLED/COMPLETED/FAILED，则状态不倒退
        if (job.isTerminal()) {
            log.info("作业 {} 已处于终态 {}，丢失事件仅清理资源，状态保持不变", job.id(), job.state());
            return;
        }

        // 丢失的不是当前 attempt（迟到的旧事件），忽略状态推进
        if (current == null || !current.attemptId().equals(lostAttemptId)) {
            log.info("作业 {} 的丢失尝试 {} 非当前尝试，忽略状态推进", job.id(), lostAttemptId);
            return;
        }

        job.currentAttempt(null);

        // 3. 重试或终止
        if (job.attemptCount() <= job.maxRetries()) {
            job.state(JobState.PENDING);
            long readyAt = time.nowMillis() + props.getRetryBackoffMillis();
            job.readyAtMillis(readyAt);
            job.queueReason("等待重试(" + job.attemptCount() + "/" + job.maxRetries()
                    + "): " + reason);
            state.eventLog().append("JOB_REQUEUED", time.nowMillis(), job.id(), nodeId,
                    lostAttemptId, List.of(reason, String.valueOf(job.attemptCount())));
            log.info("作业 {} 回到队列等待重试({}/{}), readyAt={}, 原因={}",
                    job.id(), job.attemptCount(), job.maxRetries(), readyAt, reason);
        } else {
            failJob(state, time, job, TerminationCause.MAX_RETRIES_EXHAUSTED, reason);
        }
    }

    /** 将作业置为 FAILED 终态（唯一终态、只发生一次资源结算），并连带阵列。 */
    static void failJob(SchedulerState state, TimeService time, Job job,
                        TerminationCause cause, String detail) {
        if (job.isTerminal()) {
            return;
        }
        job.state(JobState.FAILED);
        job.terminationCause(cause);
        job.detail(detail);
        job.queueReason(null);
        state.eventLog().append("JOB_FAILED", time.nowMillis(), job.id(), null,
                job.currentAttempt() == null ? null : job.currentAttempt().attemptId(),
                List.of(cause.name(), detail == null ? "" : detail));
        log.warn("作业失败终态: job={}, cause={}, detail={}", job.id(), cause, detail);

        if (job.arrayId() != null) {
            JobArray array = state.arrays().get(job.arrayId());
            if (array != null) {
                state.eventLog().append("ARRAY_FAILED", time.nowMillis(), array.id(), null, null,
                        List.of(job.id(), cause.name()));
                log.warn("阵列 {} 因任务 {} 失败而整体标记失败", array.id(), job.id());
            }
        }
    }

}


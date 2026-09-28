package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.exception.JobRejectedException;
import com.github.xingjianluo.localhpcjobschedulerjava.model.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.QueueReason;
import com.github.xingjianluo.localhpcjobschedulerjava.model.RejectCode;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerEngine;
import com.github.xingjianluo.localhpcjobschedulerjava.tests.TestSupport.Harness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 提交与校验：非法参数、超出单节点/集群容量、重复提交幂等、排队原因。
 */
class SubmissionValidationTest {

    private static final Logger log = LoggerFactory.getLogger(SubmissionValidationTest.class);

    private SchedulerEngine.SubmitOutcome submit(SchedulerEngine e, String id,
                                                int array, Resources r, long dur) {
        return e.submitJob(id, "job-" + id, array, r, dur, 0, null, null);
    }

    @Test
    void invalidRequestsAreRejectedWithDistinctReasons() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(4, 4096, 1));

        assertReject(e, "j-missing-name", () ->
                e.submitJob("j-missing-name", " ", 1, new Resources(1, 1, 0), 1000, 0, null, null),
                RejectCode.NAME_MISSING);
        assertReject(e, "j-array", () ->
                submit(e, "j-array", 0, new Resources(1, 1, 0), 1000),
                RejectCode.INVALID_ARRAY_SIZE);
        assertReject(e, "j-neg", () ->
                submit(e, "j-neg", 1, new Resources(-1, 1, 0), 1000),
                RejectCode.NEGATIVE_RESOURCE);
        assertReject(e, "j-dur", () ->
                submit(e, "j-dur", 1, new Resources(1, 1, 0), 0),
                RejectCode.INVALID_DURATION_LIMIT);
        log.info("[判定依据] 4 类非法输入分别归类为 NAME_MISSING / INVALID_ARRAY_SIZE "
                + "/ NEGATIVE_RESOURCE / INVALID_DURATION_LIMIT，且均未创建作业：jobs={}",
                e.jobs().size());
        assertEquals(0, e.jobs().size(), "被拒绝的请求不得产生作业、不得占用资源");
    }

    private void assertReject(SchedulerEngine e, String label,
                              org.junit.jupiter.api.function.Executable action,
                              RejectCode expected) {
        JobRejectedException ex = assertThrows(JobRejectedException.class, action,
                "应当被拒绝: " + label);
        log.info("[关键输入] {} -> 拒绝码 {}，原因：{}", label, ex.code(), ex.getMessage());
        assertEquals(expected, ex.code());
    }

    @Test
    void requestExceedingAnyNodeCapacityIsRejected() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        // 两个小节点，任何单节点都放不下 8 核
        e.registerNode("n1", new Resources(4, 1024, 0));
        e.registerNode("n2", new Resources(4, 1024, 0));

        var out = submit(e, "huge", 1, new Resources(8, 1, 0), 1000);
        log.info("[关键输入] 单任务请求 8 核，集群最大单节点 4 核 -> kind={} code={} message={}",
                out.kind(), out.rejectCode(), out.message());
        assertEquals(SchedulerEngine.SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectCode.EXCEEDS_MAX_NODE, out.rejectCode());
    }

    @Test
    void arrayExceedingTotalClusterCapacityIsRejected() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(8, 8192, 2));

        // 阵列 10 个任务、每个 1 核，共 10 核 > 集群 8 核
        var out = submit(e, "arr-big", 10, new Resources(1, 1, 0), 1000);
        log.info("[关键输入] 阵列 10×1 核，集群总容量 8 核 -> kind={} code={} message={}",
                out.kind(), out.rejectCode(), out.message());
        assertEquals(SchedulerEngine.SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectCode.EXCEEDS_CLUSTER_CAPACITY, out.rejectCode());
    }

    @Test
    void gangThatFitsTotalButNotAllAtOnceStaysQueuedWithReason() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        // 两个 2 核节点。阵列 2 个任务各 2 核：单任务放得下、总量也够，先占用者在；
        // 先放一个会占满节点的其它作业，使第二个任务无处可去时，阵列应整体排队。
        e.registerNode("n1", new Resources(2, 2048, 0));
        e.registerNode("n2", new Resources(2, 2048, 0));

        // 占用作业按 best-fit + nodeId 平局规则落在 n1（占满 2 核）
        submit(e, "occupy", 1, new Resources(2, 1, 0), 1000);
        assertEquals(JobState.RUNNING, e.getJob("occupy").state());
        String occupiedNode = e.getJob("occupy").tasks().get(0).nodeId();
        String freeNode = occupiedNode.equals("n1") ? "n2" : "n1";
        log.info("[关键输入] occupy 落在 {}，剩余在线节点只有 {} 可放一个 2 核任务",
                occupiedNode, freeNode);

        // 阵列需要两个 2 核：一个任务能放 freeNode，但另一个无处可去 -> 整组等待
        var out = submit(e, "gang", 2, new Resources(2, 1, 0), 1000);
        assertEquals(SchedulerEngine.SubmitOutcome.Kind.ACCEPTED, out.kind());
        assertEquals(JobState.QUEUED, e.getJob("gang").state());
        assertEquals(QueueReason.INSUFFICIENT_FOR_GANG, e.getJob("gang").queueReason());
        log.info("[判定依据] 阵列 gang 整组放置失败，排队原因={}，解释={}",
                e.getJob("gang").queueReason(), e.getJob("gang").reasonDetail());
        // 关键：不允许先占一部分——空闲节点不应被该阵列预留任何资源
        assertEquals(0, e.getNode(freeNode).used().cpus(), "整组失败时不得预留部分资源");
        assertEquals(2, e.getNode(occupiedNode).used().cpus());
    }

    @Test
    void duplicateSubmissionIsIdempotentAndDoesNotDoubleAllocate() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(4, 4096, 1));

        var first = submit(e, "dup", 1, new Resources(2, 1, 0), 1000);
        var second = submit(e, "dup", 1, new Resources(2, 1, 0), 1000);
        long usedAfterTwice = e.getNode("n1").used().cpus();
        log.info("[关键输入] 同一 jobId=dup 提交两次：第一次 kind={}，第二次 kind={}，"
                        + "节点已用核数={}（期望 2，不重复占用）",
                first.kind(), second.kind(), usedAfterTwice);
        assertEquals(SchedulerEngine.SubmitOutcome.Kind.ACCEPTED, first.kind());
        assertEquals(SchedulerEngine.SubmitOutcome.Kind.DUPLICATE, second.kind());
        assertEquals(1, e.jobs().size());
        assertEquals(2, usedAfterTwice);
        assertNotNull(e.getJob("dup"));
    }

    @Test
    void submitToEmptyClusterIsRejectedAsCapacity() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        var out = submit(e, "no-node", 1, new Resources(1, 1, 0), 1000);
        assertEquals(SchedulerEngine.SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectCode.EXCEEDS_CLUSTER_CAPACITY, out.rejectCode());
        assertFalse(out.message().isBlank());
        log.info("[判定依据] 空集群提交 -> {}：{}", out.rejectCode(), out.message());
    }
}

package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerException;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.RejectReason;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ResourceRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 提交校验：非法资源、超容量、重复提交都必须被拒绝，且原因可区分；拒绝不得污染状态。
 */
class JobValidationTests {

    private static final Logger log = LoggerFactory.getLogger(JobValidationTests.class);

    private TestHarness harness(Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 4, 8192, 1);
        h.nodeService.registerNode("n2", 4, 8192, 1);
        return h;
    }

    @Test
    void rejectsIllegalResourcesWithDistinctReasons(@TempDir Path dir) {
        TestHarness h = harness(dir);

        assertReject(h, new ResourceRequest(0, 1024, 0, 60), RejectReason.INVALID_CORES);
        assertReject(h, new ResourceRequest(-2, 1024, 0, 60), RejectReason.INVALID_CORES);
        assertReject(h, new ResourceRequest(2, 0, 0, 60), RejectReason.INVALID_MEMORY);
        assertReject(h, new ResourceRequest(2, 1024, -1, 60), RejectReason.INVALID_GPUS);
        assertReject(h, new ResourceRequest(2, 1024, 0, 0), RejectReason.INVALID_DURATION);
        assertReject(h, new ResourceRequest(2, 1024, 0, -9), RejectReason.INVALID_DURATION);

        // 所有拒绝都不得产生作业、不得占用任何资源
        assertEquals(0, h.state.jobs().size(), "非法提交不得创建作业");
        assertEquals(0, h.state.nodes().get("n1").usedCores());
        log.info("[判定依据] 6 类非法请求全部按独立原因拒绝，作业表为空，节点用量为 0");
    }

    @Test
    void rejectsRequestExceedingClusterCapacity(@TempDir Path dir) {
        TestHarness h = harness(dir);
        // 集群共 8 核，请求 16 核 -> 超总量
        SchedulerException ex = assertThrows(SchedulerException.class, () ->
                h.jobService.submitJob("big", "big",
                        new ResourceRequest(16, 1024, 0, 60), null, null));
        assertEquals(RejectReason.EXCEEDS_CLUSTER_CAPACITY, ex.reason());
        log.info("[关键输入] cores=16, 集群总核数=8 -> 原因={}, message={}",
                ex.reason(), ex.getMessage());

        // 请求 5 核：总量 8 核足够但没有任何单节点能放下（单节点 4 核）
        SchedulerException ex2 = assertThrows(SchedulerException.class, () ->
                h.jobService.submitArray("arr-big", "arr-big", 1,
                        new ResourceRequest(5, 1024, 0, 60), null, null));
        assertEquals(RejectReason.EXCEEDS_NODE_CAPACITY, ex2.reason());
        log.info("[关键输入] 阵列单任务 cores=5, 最大单节点=4 -> 原因={}", ex2.reason());

        // 阵列整组超过集群总量：2 个任务 x 6 核 = 12 > 8
        SchedulerException ex3 = assertThrows(SchedulerException.class, () ->
                h.jobService.submitArray("arr-sum", "arr-sum", 2,
                        new ResourceRequest(6, 1024, 0, 60), null, null));
        assertEquals(RejectReason.EXCEEDS_CLUSTER_CAPACITY, ex3.reason());
        log.info("[关键输入] 阵列 2x6核=12 > 集群 8 核 -> 原因={}", ex3.reason());

        assertEquals(0, h.state.jobs().size(), "被拒阵列不得留下任务");
        assertEquals(0, h.state.arrays().size());
    }

    @Test
    void duplicateSubmissionIsRejectedAndDoesNotDoubleAllocate(@TempDir Path dir) {
        TestHarness h = harness(dir);
        Job first = h.jobService.submitJob("dup-key", "c1",
                new ResourceRequest(2, 1024, 0, 60), null, null);
        h.schedulerService.tick();
        int usedAfterFirst = h.state.nodes().get("n1").usedCores();
        log.info("[关键输入] 首次提交 job={} 调度后 n1 已用核数={}", first.id(), usedAfterFirst);
        assertTrue(usedAfterFirst > 0);

        SchedulerException ex = assertThrows(SchedulerException.class, () ->
                h.jobService.submitJob("dup-key", "c1",
                        new ResourceRequest(2, 1024, 0, 60), null, null));
        assertEquals(RejectReason.DUPLICATE_SUBMISSION, ex.reason());

        int usedAfterDup = h.state.nodes().get("n1").usedCores();
        assertEquals(usedAfterFirst, usedAfterDup, "重复提交不得重复占用资源");
        assertEquals(1, h.state.jobs().size(), "重复提交不得新建作业");
        log.info("[判定依据] 重复提交被 {} 拒绝，n1 用量 {} -> {} 未变化",
                ex.reason(), usedAfterFirst, usedAfterDup);
    }

    @Test
    void duplicateArraySubmissionIsIdempotent(@TempDir Path dir) {
        TestHarness h = harness(dir);
        List<Job> tasks = h.jobService.submitArray("arr-key", "ca", 3,
                new ResourceRequest(1, 1024, 0, 60), null, null);
        SchedulerException ex = assertThrows(SchedulerException.class, () ->
                h.jobService.submitArray("arr-key", "ca", 3,
                        new ResourceRequest(1, 1024, 0, 60), null, null));
        assertEquals(RejectReason.DUPLICATE_SUBMISSION, ex.reason());
        assertEquals(3, h.state.jobs().size());
        assertEquals(1, h.state.arrays().size());
        assertEquals(3, tasks.size());
        log.info("[判定依据] 阵列重复提交被拒，任务数仍为 3，阵列数仍为 1");
    }

    private void assertReject(TestHarness h, ResourceRequest req, RejectReason expected) {
        SchedulerException ex = assertThrows(SchedulerException.class, () ->
                h.jobService.submitJob("k-" + req.cores() + "-" + req.memoryMb()
                        + "-" + req.gpus() + "-" + req.durationLimitSec(),
                        null, req, null, null));
        assertEquals(expected, ex.reason(),
                "请求 " + req + " 应被归类为 " + expected + " 实际 " + ex.reason());
        log.info("[关键输入] {} -> 拒绝原因 {}（{}）", req, ex.reason(), ex.getMessage());
    }
}

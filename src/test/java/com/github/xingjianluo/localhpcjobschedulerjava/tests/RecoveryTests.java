package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.domain.ComputeNode;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ResourceRequest;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.TerminationCause;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 进程重启恢复：从本地事件日志重放后
 * - 节点、容量、终态作业、排队作业全部还原；
 * - 崩溃前 RUNNING 的尝试成为孤儿，按丢失策略重试/终止，资源对账一致；
 * - 恢复后系统可继续正常调度与完成，不重复占资源。
 */
class RecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(RecoveryTests.class);

    @Test
    void recoversAndReconcilingOrphanAttempts(@TempDir Path dir) throws Exception {
        Path logFile = dir.resolve("events.log");

        // === 第一个"进程"生命周期 ===
        TestHarness h1 = TestHarness.create(logFile);
        h1.nodeService.registerNode("n1", 4, 8192, 0);
        h1.nodeService.registerNode("n2", 4, 8192, 0);

        Job willComplete = h1.jobService.submitJob("done", "done",
                new ResourceRequest(1, 256, 0, 60), 0, 0);
        Job willCancel = h1.jobService.submitJob("canceled", "canceled",
                new ResourceRequest(1, 256, 0, 60), 0, 0);
        Job runningAtCrash = h1.jobService.submitJob("orphan", "orphan",
                new ResourceRequest(2, 512, 0, 60), 0, 2); // 允许重试 2 次
        Job pendingAtCrash = h1.jobService.submitJob("queued", "queued",
                new ResourceRequest(1, 256, 0, 60), 0, 0);
        h1.schedulerService.tick();

        h1.jobService.complete(willComplete.id(), willComplete.currentAttempt().attemptId());
        h1.jobService.cancel(willCancel.id(), "before-crash-cancel");
        // runningAtCrash 保持 RUNNING（模拟进程在其运行期间崩溃）；pendingAtCrash 保持 PENDING
        JobState orphanBefore = runningAtCrash.state();
        String orphanAttempt = runningAtCrash.currentAttempt().attemptId();
        String orphanNode = runningAtCrash.currentAttempt().nodeId();
        assertEquals(JobState.RUNNING, orphanBefore);
        log.info("[崩溃前快照] completed={}, cancelled={}, RUNNING(attempt={},node={}), PENDING=1",
                willComplete.id(), willCancel.id(), orphanAttempt, orphanNode);

        // === 模拟进程重启：全新内存状态，从同一事件日志恢复 ===
        TestHarness h2 = h1.restart(5_000_000L);
        assertEquals(2, h2.state.nodes().size(), "节点从日志还原");
        assertEquals(4, h2.state.jobs().size(), "作业全部还原");

        Job done2 = h2.jobService.getJob(willComplete.id());
        Job canceled2 = h2.jobService.getJob(willCancel.id());
        Job orphan2 = h2.jobService.getJob(runningAtCrash.id());
        Job queued2 = h2.jobService.getJob(pendingAtCrash.id());

        assertEquals(JobState.COMPLETED, done2.state());
        assertEquals(TerminationCause.COMPLETED_BY_WORKER, done2.terminationCause());
        assertEquals(JobState.CANCELLED, canceled2.state());
        assertEquals(TerminationCause.CANCELLED_BY_CLIENT, canceled2.terminationCause());
        // recover 只重建状态与收敛孤儿，不主动调度：孤儿回到 PENDING（旧 attempt 已释放）
        assertEquals(JobState.PENDING, orphan2.state(),
                "崩溃前 RUNNING 的孤儿尝试恢复后应回 PENDING，等待下一轮调度重试");
        assertNull(orphan2.currentAttempt());
        assertNotNull(queued2);
        log.info("[恢复后状态] done=COMPLETED, canceled=CANCELLED, orphan=PENDING(旧attempt已释放), queued={}",
                queued2.state());

        assertFalse(h2.state.attemptIndex().containsKey(orphanAttempt),
                "旧孤儿 attempt 索引必须清除");

        // 资源对账：恢复完成、尚未重新调度时，没有任何作业占用资源
        assertNoNegativeUsage(h2);
        assertEquals(0, totalUsedCores(h2), "恢复后未调度前，旧孤儿/终态作业均不占资源");

        // 下一轮调度：孤儿以新 attempt 重新运行（重试不重复计资源）
        h2.schedulerService.tick();
        assertEquals(JobState.RUNNING, orphan2.state());
        assertNotEquals(orphanAttempt, orphan2.currentAttempt().attemptId(),
                "恢复后运行的必须是新 attempt，旧孤儿 attempt 已释放");
        assertEquals(2, orphan2.attemptCount());
        log.info("[判定依据] 孤儿 attempt 恢复后经一轮 tick 重试为 attempt#2（{}），旧 attempt 不再记账",
                orphan2.currentAttempt().attemptId());

        int expectedCores = (int) h2.state.jobs().values().stream()
                .filter(j -> j.state() == JobState.RUNNING)
                .mapToInt(j -> j.request().cores()).sum();
        assertEquals(expectedCores, totalUsedCores(h2), "恢复后用量与运行中作业严格一致");
        log.info("[恢复对账] 重新调度后记账已用核数={}（与运行中请求合计一致）", expectedCores);

        // 恢复后继续正常推进：让所有作业跑完，最终资源归零
        int guard = 0;
        while (hasPending(h2) && guard++ < 10) {
            h2.schedulerService.tick();
            completeAllRunning(h2);
        }
        h2.schedulerService.tick();
        assertEquals(0, totalUsedCores(h2), "恢复后继续运行直至清空，资源最终归零");
        for (Job j : h2.state.jobs().values()) {
            assertTrue(j.isTerminal(),
                    j.id() + " 未达终态: " + j.state() + ", cause=" + j.terminationCause());
        }
        long completed = h2.state.jobs().values().stream()
                .filter(j -> j.state() == JobState.COMPLETED).count();
        long failed = h2.state.jobs().values().stream()
                .filter(j -> j.state() == JobState.FAILED).count();
        log.info("[判定依据] 恢复后继续推进至全部终态：COMPLETED={}, FAILED={}(容量长期不足耗尽重试), CANCELLED=1，集群资源=0",
                completed, failed);
        log.info("[判定依据] 恢复后继续调度直至全部终态，集群资源=0，无重复占用");
    }

    @Test
    void recoveryIsDeterministicAcrossRepeatedReplays(@TempDir Path dir) {
        Path logFile = dir.resolve("events.log");
        TestHarness h1 = TestHarness.create(logFile);
        h1.nodeService.registerNode("n1", 4, 8192, 0);
        h1.jobService.submitJob("j1", "j1", new ResourceRequest(2, 512, 0, 60), 0, 1);
        h1.schedulerService.tick(); // RUNNING 中崩溃

        TestHarness r1 = h1.restart(9_000_000L);
        int used1 = totalUsedCores(r1);
        TestHarness r2 = h1.restart(9_000_000L);
        int used2 = totalUsedCores(r2);
        assertEquals(used1, used2, "同一日志多次重放结果必须一致");
        log.info("[判定依据] 同一事件日志两次独立重放，记账用量一致={}核", used1);
    }

    @Test
    void arrayStateRecoversAsWhole(@TempDir Path dir) {
        Path logFile = dir.resolve("events.log");
        TestHarness h1 = TestHarness.create(logFile);
        h1.nodeService.registerNode("n1", 4, 8192, 0);
        h1.nodeService.registerNode("n2", 4, 8192, 0);
        List<Job> tasks = h1.jobService.submitArray("arr", "arr", 2,
                new ResourceRequest(2, 512, 0, 60), 0, 1);
        h1.schedulerService.tick();
        assertEquals(JobState.RUNNING, tasks.get(0).state());

        TestHarness h2 = h1.restart(7_000_000L);
        assertEquals(1, h2.state.arrays().size());
        Job t0 = h2.jobService.getJob(tasks.get(0).id());
        Job t1 = h2.jobService.getJob(tasks.get(1).id());
        // 两个孤儿任务一起收敛、一起重新调度（恢复后 tick 保持整组语义）
        h2.schedulerService.tick();
        boolean bothRunning = t0.state() == JobState.RUNNING && t1.state() == JobState.RUNNING;
        boolean bothPending = t0.state() == JobState.PENDING && t1.state() == JobState.PENDING;
        assertTrue(bothRunning || bothPending, "阵列任务恢复后仍须同进同退");
        log.info("[判定依据] 阵列恢复后两个任务状态一致: {} / {}", t0.state(), t1.state());
    }

    private boolean hasPending(TestHarness h) {
        return h.state.jobs().values().stream().anyMatch(j -> !j.isTerminal());
    }

    private void completeAllRunning(TestHarness h) {
        for (Job j : h.state.jobs().values()) {
            if (j.state() == JobState.RUNNING && j.currentAttempt() != null) {
                h.jobService.complete(j.id(), j.currentAttempt().attemptId());
            }
        }
    }

    private void assertNoNegativeUsage(TestHarness h) {
        for (ComputeNode n : h.state.nodes().values()) {
            assertTrue(n.usedCores() >= 0 && n.usedMemoryMb() >= 0 && n.usedGpus() >= 0);
        }
    }

    private int totalUsedCores(TestHarness h) {
        int sum = 0;
        for (ComputeNode n : h.state.nodes().values()) {
            sum += n.usedCores();
        }
        return sum;
    }
}

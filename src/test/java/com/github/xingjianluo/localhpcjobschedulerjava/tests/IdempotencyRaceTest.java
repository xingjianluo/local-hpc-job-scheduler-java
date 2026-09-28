package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.exception.ConflictException;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.model.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerEngine;
import com.github.xingjianluo.localhpcjobschedulerjava.tests.TestSupport.Harness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 取消与完成竞争、重复心跳/完成、并发提交与取消：终态唯一、资源只释放一次。
 */
class IdempotencyRaceTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyRaceTest.class);

    @Test
    void duplicateCompleteIsIdempotentAndReleasesResourcesOnce() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(8, 8192, 0));
        e.submitJob("j", "作业", 1, new Resources(3, 1, 0), 1000, 0, null, null);
        assertEquals(3, e.getNode("n1").used().cpus());

        Job afterFirst = e.taskComplete("j.0");
        long usedAfterFirst = e.getNode("n1").used().cpus();
        Job afterSecond = e.taskComplete("j.0"); // 重复完成上报
        Job afterThird = e.taskComplete("j.0");
        long usedAfterThird = e.getNode("n1").used().cpus();
        log.info("[关键输入] 连续 3 次完成上报：状态 {} -> {} -> {}；节点已用 {} -> {}",
                afterFirst.state(), afterSecond.state(), afterThird.state(),
                usedAfterFirst, usedAfterThird);
        assertEquals(JobState.COMPLETED, afterThird.state());
        assertEquals(0, usedAfterFirst);
        assertEquals(0, usedAfterThird, "重复完成不得第二次释放资源");
    }

    @Test
    void cancelAndCompleteRaceConvergesToExactlyOneTerminalState() throws Exception {
        // 50 个线程：一半取消、一半完成同一单任务作业，反复运行多轮
        for (int round = 0; round < 20; round++) {
            Harness h = TestSupport.engine();
            SchedulerEngine e = h.engine();
            e.registerNode("n1", new Resources(8, 8192, 0));
            e.submitJob("race", "竞争作业", 1, new Resources(2, 1, 0), 1000, 0, null, null);

            int threads = 20;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            AtomicInteger completes = new AtomicInteger();
            AtomicInteger cancels = new AtomicInteger();
            AtomicInteger conflicts = new AtomicInteger();
            for (int i = 0; i < threads; i++) {
                boolean cancel = i % 2 == 0;
                pool.submit(() -> {
                    try {
                        start.await();
                        if (cancel) {
                            e.cancelJob("race");
                            cancels.incrementAndGet();
                        } else {
                            e.taskComplete("race.0");
                            completes.incrementAndGet();
                        }
                    } catch (ConflictException conflict) {
                        // 取消与完成竞争落败方：正常现象，状态不得被污染
                        conflicts.incrementAndGet();
                    } catch (Exception other) {
                        log.error("未预期异常", other);
                        throw new RuntimeException(other);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
            pool.shutdown();

            JobState finalState = e.getJob("race").state();
            long used = e.getNode("n1").used().cpus();
            assertTrue(finalState == JobState.COMPLETED || finalState == JobState.CANCELLED,
                    "终态必须唯一且为 COMPLETED 或 CANCELLED，实际=" + finalState);
            assertEquals(0, used, "无论谁胜出，资源必须恰好释放一次，实际已用=" + used);
            // 终态操作不可逆：再取消/再完成都只能得到冲突或幂等
            if (round == 0) {
                log.info("[判定依据] 首轮并发收敛：终态={}，成功取消={}，成功完成={}，竞争落败={}，节点已用={}",
                        finalState, cancels.get(), completes.get(), conflicts.get(), used);
            }
        }
        log.info("[判定依据] 20 轮取消/完成并发竞争全部收敛到唯一终态且资源归零");
    }

    @Test
    void cancelQueuedOrRunningArrayReleasesOnlyRunningBindings() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(4, 4096, 0));

        // 运行中的 2 核作业
        e.submitJob("run", "运行", 1, new Resources(2, 1, 0), 1000, 0, null, null);
        // 排不上的 4 核作业（剩 2 核不够）
        e.submitJob("wait", "等待", 1, new Resources(4, 1, 0), 1000, 0, null, null);
        assertEquals(JobState.QUEUED, e.getJob("wait").state());

        e.cancelJob("wait");
        assertEquals(JobState.CANCELLED, e.getJob("wait").state());
        assertEquals(2, e.getNode("n1").used().cpus(), "取消排队作业不影响在运行作业的用量");

        e.cancelJob("run");
        assertEquals(0, e.getNode("n1").used().cpus(), "取消运行作业释放其占用");
        assertEquals(JobState.CANCELLED, e.getJob("run").state());

        // 终态不可逆
        ConflictException ex = org.junit.jupiter.api.Assertions.assertThrows(
                ConflictException.class, () -> e.cancelJob("run"));
        log.info("[判定依据] 对已取消作业再取消被拒绝：{}", ex.getMessage());
    }

    @Test
    void concurrentDuplicateSubmissionsOfSameIdCreateExactlyOneJob() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(64, 65536, 8));

        int threads = 32;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    var out = e.submitJob("same-id", "并发重复提交", 1,
                            new Resources(1, 1, 0), 1000, 0, null, null);
                    if (out.kind() == SchedulerEngine.SubmitOutcome.Kind.ACCEPTED) {
                        accepted.incrementAndGet();
                    } else if (out.kind() == SchedulerEngine.SubmitOutcome.Kind.DUPLICATE) {
                        duplicate.incrementAndGet();
                    }
                } catch (Exception ex) {
                    log.error("未预期异常", ex);
                    throw new RuntimeException(ex);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        log.info("[判定依据] 32 个并发相同 jobId 提交：接受={}，幂等重复={}，作业总数={}，节点已用核数={}",
                accepted.get(), duplicate.get(), e.jobs().size(), e.getNode("n1").used().cpus());
        assertEquals(1, accepted.get(), "恰好一个提交真正生效");
        assertEquals(threads - 1, duplicate.get());
        assertEquals(1, e.jobs().size());
        assertEquals(1, e.getNode("n1").used().cpus(), "重复提交绝不重复占用资源");
    }

    @Test
    void concurrentCancelsAndCompletesAcrossManyJobsNeverOverRelease() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(256, 262144, 32));
        int jobCount = 40;
        for (int i = 0; i < jobCount; i++) {
            e.submitJob("j" + i, "作业" + i, 1, new Resources(2, 1, 0), 1000, 0, null, null);
        }
        assertEquals(jobCount * 2L, e.getNode("n1").used().cpus());

        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(jobCount * 2);
        for (int i = 0; i < jobCount; i++) {
            final String id = "j" + i;
            pool.submit(() -> awaitAndRun(start, done, () -> {
                try {
                    e.cancelJob(id);
                } catch (ConflictException ignore) {
                    // 与完成竞争落败
                }
            }));
            pool.submit(() -> awaitAndRun(start, done, () -> {
                try {
                    e.taskComplete(id + ".t0");
                } catch (ConflictException ignore) {
                    // 与取消竞争落败
                }
            }));
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        long used = e.getNode("n1").used().cpus();
        long terminal = e.jobs().stream().filter(Job::isTerminal).count();
        log.info("[判定依据] 40 作业×(取消+完成) 并发：终态作业 {} 个，节点已用核数={}（必须 0）",
                terminal, used);
        assertEquals(0, used, "绝不允许超额/重复释放导致用量为负或残留");
        assertEquals(jobCount, terminal);
    }

    private void awaitAndRun(CountDownLatch start, CountDownLatch done, Runnable action) {
        try {
            start.await();
            action.run();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            done.countDown();
        }
    }
}

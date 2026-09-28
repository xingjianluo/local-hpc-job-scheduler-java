package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerException;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ComputeNode;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.RejectReason;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ResourceRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发异常链路：
 * - 大量作业并发提交 + 多线程同时 tick：节点容量绝不超额；
 * - 同一作业被多线程重复提交：恰好一份成功，其余按 DUPLICATE_SUBMISSION 拒绝；
 * - 运行中作业被多线程同时取消/完成：收敛唯一终态，资源恰好释放一次；
 * - 失败操作不得污染已有状态。
 */
class ConcurrencyTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTests.class);

    @Test
    void concurrentSubmissionsNeverOverAllocate(@TempDir Path dir) throws Exception {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 8, 16384, 0);
        h.nodeService.registerNode("n2", 8, 16384, 0);

        int threads = 16;
        int perThread = 20; // 共 320 个 1 核作业，集群仅 16 核
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger submitted = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    int id = base + i;
                    h.jobService.submitJob("job-" + id, "c-" + id,
                            new ResourceRequest(1, 256, 0, 60), 0, 0);
                    submitted.incrementAndGet();
                    if (id % 3 == 0) {
                        h.schedulerService.tick(); // 提交与调度并发交错
                    }
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        h.schedulerService.tick();

        assertEquals(320, submitted.get());
        int running = 0;
        int pending = 0;
        for (Job job : h.state.jobs().values()) {
            if (job.state() == JobState.RUNNING) {
                running++;
            } else if (job.state() == JobState.PENDING) {
                pending++;
            }
        }
        int used = totalUsedCores(h);
        assertEquals(16, running, "恰好填满 16 核");
        assertEquals(16, used, "已分配核数必须等于运行中作业数，绝不超额");
        assertEquals(304, pending, "其余作业全部留队排队");
        for (ComputeNode n : h.state.nodes().values()) {
            assertTrue(n.usedCores() <= n.totalCores(),
                    "节点 " + n.id() + " 超额: " + n.usedCores() + "/" + n.totalCores());
            assertEquals(n.usedCores(), n.runningAttempts().size(),
                    "记账用量与运行尝试数逐节点一致");
        }
        log.info("[判定依据] {}线程并发提交320个1核作业+交错调度：RUNNING={}, PENDING={}, 已用核={}（=容量16，无超额）",
                threads, running, pending, used);
    }

    @Test
    void concurrentDuplicateSubmissionsExactlyOneWins(@TempDir Path dir) throws Exception {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 8, 16384, 0);

        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger dupReject = new AtomicInteger();
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                start.await();
                try {
                    h.jobService.submitJob("same-key", "same-client",
                            new ResourceRequest(2, 512, 0, 60), 0, 0);
                    success.incrementAndGet();
                } catch (SchedulerException ex) {
                    if (ex.reason() == RejectReason.DUPLICATE_SUBMISSION) {
                        dupReject.incrementAndGet();
                    } else {
                        errors.add("意外原因: " + ex.reason());
                    }
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        assertEquals(1, success.get(), "同一作业并发重复提交必须恰好一份成功");
        assertEquals(threads - 1, dupReject.get(), "其余全部按 DUPLICATE_SUBMISSION 拒绝");
        assertTrue(errors.isEmpty(), errors.toString());
        assertEquals(1, h.state.jobs().size());
        h.schedulerService.tick();
        assertEquals(2, totalUsedCores(h), "资源只被成功的一份占用 2 核");
        log.info("[判定依据] 20线程重复提交同一作业：成功1份，DUPLICATE_SUBMISSION={}份，作业表1条，占用2核",
                dupReject.get());
    }

    @Test
    void concurrentCancelAndCompleteConvergeOnce(@TempDir Path dir) throws Exception {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 8, 16384, 0);
        int jobCount = 50;
        for (int i = 0; i < jobCount; i++) {
            h.jobService.submitJob("j-" + i, "c-" + i,
                    new ResourceRequest(1, 256, 0, 60), 0, 0);
        }
        h.schedulerService.tick();
        assertEquals(8, totalUsedCores(h));

        // 对 8 个运行中作业，每个作业各起两个线程：一个取消、一个完成，重复多次
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        Job[] running = h.state.jobs().values().stream()
                .filter(j -> j.state() == JobState.RUNNING)
                .toArray(Job[]::new);
        assertEquals(8, running.length);
        for (Job job : running) {
            for (int round = 0; round < 3; round++) {
                pool.submit(guard(start, () -> h.jobService.cancel(job.id(), "concurrent-cancel")));
                pool.submit(guard(start, () -> h.jobService.complete(
                        job.id(), job.currentAttempt() == null ? "stale" : job.currentAttempt().attemptId())));
            }
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        for (Job job : running) {
            assertTrue(job.state() == JobState.CANCELLED || job.state() == JobState.COMPLETED,
                    "每个作业必须落入唯一终态，实际=" + job.state());
            assertNotNull(job.terminationCause());
        }
        int used = totalUsedCores(h);
        assertEquals(0, used, "无论取消还是完成，全部终态作业的资源合计必须为 0（只释放一次）");
        assertEquals(0, h.state.attemptIndex().size(), "attempt 索引全部清空，无悬挂记账");
        // 排队作业不受影响、状态无污染
        long pendingStill = h.state.jobs().values().stream()
                .filter(j -> j.state() == JobState.PENDING).count();
        assertEquals(42, pendingStill, "未参与竞争的排队作业状态保持 PENDING");
        log.info("[判定依据] 8个运行中作业各自并发取消/完成各3轮：全部唯一终态，资源释放后=0，42个排队作业不受污染");
    }

    private Runnable guard(CountDownLatch start, Runnable r) {
        return () -> {
            try {
                start.await();
                r.run();
            } catch (Exception ignored) {
                // 终态竞争中的拒绝/忽略是预期行为
            }
        };
    }

    private int totalUsedCores(TestHarness h) {
        int sum = 0;
        for (ComputeNode n : h.state.nodes().values()) {
            sum += n.usedCores();
        }
        return sum;
    }
}

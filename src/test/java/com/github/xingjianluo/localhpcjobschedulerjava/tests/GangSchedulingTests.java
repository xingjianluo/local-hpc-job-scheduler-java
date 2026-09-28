package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.domain.ComputeNode;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerException;
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
 * 整组调度规则：
 * - 阵列要么一次性全部获得资源，要么整体等待，不允许部分占用；
 * - 资源不足时排队并说明原因；
 * - 优先级 + 先到先得 + 防饥饿老化，结果确定可复现。
 */
class GangSchedulingTests {

    private static final Logger log = LoggerFactory.getLogger(GangSchedulingTests.class);

    @Test
    void arrayIsScheduledAllOrNothing(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        // 两个节点各 4 核：共 8 核
        h.nodeService.registerNode("n1", 4, 8192, 0);
        h.nodeService.registerNode("n2", 4, 8192, 0);

        // 阵列 3 个任务 x 4 核：总量 12 > 集群 8 -> 提交即被拒（永久不可满足，原因可区分）
        SchedulerException capEx = assertThrows(SchedulerException.class, () ->
                h.jobService.submitArray("arr-too-big", "arr-too-big", 3,
                        new ResourceRequest(4, 1024, 0, 60), null, null));
        assertEquals(RejectReason.EXCEEDS_CLUSTER_CAPACITY, capEx.reason());
        log.info("[关键输入] 3x4核=12 > 集群8核，提交即拒绝: {}", capEx.reason());

        // 排队场景：2 任务 x 4 核恰好整组可放；先让一个 4 核占位作业占掉 n1，
        // 此时只剩 n2 的 4 核，整组(需两个节点同时各出4核)放不下 -> 整体排队、0 占用
        Job occupier = h.jobService.submitJob("occ", "occ",
                new ResourceRequest(4, 1024, 0, 60), 100, 0);
        List<Job> tasks = h.jobService.submitArray("arr", "arr", 2,
                new ResourceRequest(4, 1024, 0, 60), null, null);
        h.schedulerService.tick(); // 先调度高优先级占位作业
        assertEquals(JobState.RUNNING, occupier.state());
        // 阵列此时应整体排队
        for (Job t : tasks) {
            assertEquals(JobState.PENDING, t.state());
            assertNotNull(t.queueReason());
            assertTrue(t.queueReason().contains("整组"), "排队原因须说明整组不足: " + t.queueReason());
        }
        int totalUsed = totalUsedCores(h);
        assertEquals(4, totalUsed, "整体等待时不得先占部分资源（仅占位作业占4核）");
        log.info("[判定依据] 2x4核阵列因节点n1被占而整体排队，排队原因='{}'，集群已用核数={}",
                tasks.get(0).queueReason(), totalUsed);

        // 占位完成释放容量后，下一轮整组立即同时启动
        h.jobService.complete(occupier.id(), occupier.currentAttempt().attemptId());
        int startedNow = h.schedulerService.tick();
        assertEquals(2, startedNow, "容量恢复后整组必须一次性同时启动");
        for (Job t : tasks) {
            assertEquals(JobState.RUNNING, t.state());
        }
        assertEquals(8, totalUsedCores(h));
        log.info("[判定依据] 容量恢复后阵列 2 个任务同轮启动，集群已用核数=8");

        // 释放集群容量的场景：阵列改为 2 任务 x 4 核 -> 每个节点各放一个，整组成功
        TestHarness h2 = TestHarness.create(dir.resolve("events2.log"));
        h2.nodeService.registerNode("n1", 4, 8192, 0);
        h2.nodeService.registerNode("n2", 4, 8192, 0);
        List<Job> tasks2 = h2.jobService.submitArray("arr2", "arr2", 2,
                new ResourceRequest(4, 1024, 0, 60), null, null);
        int started2 = h2.schedulerService.tick();
        assertEquals(2, started2, "整组可放下时必须全部同时启动");
        for (Job t : tasks2) {
            assertEquals(JobState.RUNNING, t.state());
        }
        assertEquals(8, totalUsedCores(h2), "整组 8 核全部落账");
        log.info("[判定依据] 2x4核阵列整组启动，两个节点各承载 1 个任务，集群已用核数=8");
    }

    @Test
    void noPartialAllocationWhenFragmented(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 6, 8192, 0);
        h.nodeService.registerNode("n2", 6, 8192, 0);

        // 先占掉每个节点 3 核：每节点剩 3 核，总空闲 6 核
        Job occ1 = h.jobService.submitJob("occ1", "occ1",
                new ResourceRequest(3, 1024, 0, 60), null, null);
        Job occ2 = h.jobService.submitJob("occ2", "occ2",
                new ResourceRequest(3, 1024, 0, 60), null, null);
        h.schedulerService.tick();
        assertEquals(JobState.RUNNING, occ1.state());
        assertEquals(JobState.RUNNING, occ2.state());

        // 阵列 2 任务 x 4 核：总量 8 > 空闲 6，且无单节点剩 4 核 -> 必须整体等待，0 占用
        List<Job> tasks = h.jobService.submitArray("frag", "frag", 2,
                new ResourceRequest(4, 1024, 0, 60), null, null);
        int started = h.schedulerService.tick();
        assertEquals(0, started);
        assertEquals(6, totalUsedCores(h), "碎片导致整组失败时不得新占任何资源");
        assertTrue(tasks.get(0).queueReason().contains("碎片")
                || tasks.get(0).queueReason().contains("CPU"));
        log.info("[判定依据] 每节点剩3核，2x4核阵列无法整组放置，排队原因='{}'，已用仍为6",
                tasks.get(0).queueReason());
    }

    @Test
    void priorityOrderingIsDeterministicAndAgingPreventsStarvation(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 4, 8192, 0);

        // 占满 4 核的大作业（低优先级先到）
        Job big = h.jobService.submitJob("big", "big",
                new ResourceRequest(4, 1024, 0, 60), 0, 0);
        h.schedulerService.tick();
        assertEquals(JobState.RUNNING, big.state());

        // 高优先级后到 vs 低优先级先到：资源仅够 1 核任务依次运行
        Job lateHigh = h.jobService.submitJob("high", "high",
                new ResourceRequest(1, 1024, 0, 60), 10, 0);
        h.clock.advance(10);
        Job earlyLow = h.jobService.submitJob("low", "low",
                new ResourceRequest(1, 1024, 0, 60), 1, 0);
        h.schedulerService.tick();
        // 无空闲容量，二者都排队
        assertEquals(JobState.PENDING, lateHigh.state());
        assertEquals(JobState.PENDING, earlyLow.state());

        // 大作业完成，释放 4 核；下一轮高优先级先得
        h.jobService.complete(big.id(), big.currentAttempt().attemptId());
        h.schedulerService.tick();
        assertEquals(JobState.RUNNING, lateHigh.state(), "高优先级应先被调度");
        assertEquals(JobState.RUNNING, earlyLow.state(), "剩余容量足够，低优先级随后也启动");
        log.info("[判定依据] 大作业释放后，高优先级作业先于低优先级启动（同轮容量足够两者都启动）");

        // 防饥饿：低优先级作业等待超过老化阈值后，有效优先级反超并先被调度
        TestHarness h2 = TestHarness.create(dir.resolve("events2.log"));
        h2.props.setStarvationBoostMillis(100);
        h2.nodeService.registerNode("n1", 4, 8192, 0);
        Job blocker = h2.jobService.submitJob("blocker", "blocker",
                new ResourceRequest(4, 1024, 0, 60), 0, 0);
        h2.schedulerService.tick();
        Job starving = h2.jobService.submitJob("starving", "starving",
                new ResourceRequest(4, 1024, 0, 60), 0, 0);
        h2.clock.advance(1000); // 老作业先排队等待 1000ms，获得 10 级老化提升
        Job fresh = h2.jobService.submitJob("fresh", "fresh",
                new ResourceRequest(4, 1024, 0, 60), 5, 0);
        h2.jobService.complete(blocker.id(), blocker.currentAttempt().attemptId());
        h2.schedulerService.tick();
        assertEquals(JobState.RUNNING, starving.state(), "老化提升应让长期等待的低优先级作业先运行");
        assertEquals(JobState.PENDING, fresh.state());
        log.info("[判定依据] 先到作业等待1000ms(阈值100ms)老化+10，反超静态优先级5的后到作业，先被调度");
    }

    @Test
    void repeatedTickProducesSameDeterministicResult(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 2, 8192, 0);
        h.jobService.submitJob("a", "a", new ResourceRequest(2, 1024, 0, 60), 0, 0);
        h.jobService.submitJob("b", "b", new ResourceRequest(2, 1024, 0, 60), 0, 0);
        int first = h.schedulerService.tick();
        int second = h.schedulerService.tick();
        int third = h.schedulerService.tick();
        assertEquals(1, first);
        assertEquals(0, second, "容量未变化时重复 tick 结果确定为 0 启动");
        assertEquals(0, third);
        log.info("[判定依据] 连续 3 轮 tick 启动数依次为 1,0,0，调度结果确定");
    }

    private int totalUsedCores(TestHarness h) {
        int sum = 0;
        for (ComputeNode n : h.state.nodes().values()) {
            sum += n.usedCores();
        }
        return sum;
    }
}

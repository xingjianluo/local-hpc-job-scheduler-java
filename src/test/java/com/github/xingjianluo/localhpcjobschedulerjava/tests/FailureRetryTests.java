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

import static org.junit.jupiter.api.Assertions.*;

/**
 * 异常收敛链路：
 * - 节点掉线 -> 在跑作业丢失 -> 按 maxRetries 重试或终止；
 * - 心跳超时同样触发收敛；
 * - 重试不得重复计资源（丢失瞬间释放旧 attempt，重新调度只占一次）；
 * - 重复掉线通知幂等。
 */
class FailureRetryTests {

    private static final Logger log = LoggerFactory.getLogger(FailureRetryTests.class);

    @Test
    void jobRetriesOnNodeLostWithoutDoubleAccounting(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 4, 8192, 0);
        h.nodeService.registerNode("n2", 4, 8192, 0);

        Job job = h.jobService.submitJob("j1", "j1",
                new ResourceRequest(4, 1024, 0, 60), null, 2); // maxRetries=2
        h.schedulerService.tick();
        assertEquals(JobState.RUNNING, job.state());
        String firstAttempt = job.currentAttempt().attemptId();
        String firstNode = job.currentAttempt().nodeId();
        assertEquals(4, h.state.nodes().get(firstNode).usedCores());
        log.info("[关键输入] job={} 在 {} 上运行，attempt#1={}，该节点已用4核",
                job.id(), firstNode, firstAttempt);

        // 节点掉线：旧尝试资源释放，作业回到 PENDING
        int lost = h.nodeService.markNodeLost(firstNode, "power-off");
        assertEquals(1, lost);
        assertEquals(JobState.PENDING, job.state());
        assertEquals(0, h.state.nodes().get(firstNode).usedCores(), "丢失后旧节点用量必须归零");
        assertEquals(1, job.attemptCount(), "回队阶段尝试次数仍为 1，尚未重试");
        log.info("[判定依据] 掉线后 job 回到 PENDING，{} 已用核数=0，排队原因='{}'",
                firstNode, job.queueReason());

        // 下一轮在另一节点重新运行（重试 attempt#2），全集群只占一份资源
        h.schedulerService.tick();
        assertEquals(JobState.RUNNING, job.state());
        assertEquals(2, job.attemptCount(), "重新调度产生第 2 次尝试");
        assertNotEquals(firstAttempt, job.currentAttempt().attemptId());
        String secondNode = job.currentAttempt().nodeId();
        assertNotEquals(firstNode, secondNode, "重试应落到在线节点");
        int totalUsed = totalUsed(h);
        assertEquals(4, totalUsed, "重试只占一份资源（4核），旧尝试未重复计账");
        log.info("[判定依据] 重试运行于 {} attempt#2={}，全集群总占用={}核（无双倍记账）",
                secondNode, job.currentAttempt().attemptId(), totalUsed);

        // 完成后资源释放、终态 COMPLETED
        h.jobService.complete(job.id(), job.currentAttempt().attemptId());
        assertEquals(JobState.COMPLETED, job.state());
        assertEquals(0, totalUsed(h));
    }

    @Test
    void jobFailsAfterRetriesExhausted(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 4, 8192, 0);
        h.nodeService.registerNode("n2", 4, 8192, 0);

        Job job = h.jobService.submitJob("j1", "j1",
                new ResourceRequest(4, 1024, 0, 60), null, 1); // 只允许重试 1 次
        h.schedulerService.tick();
        String n1st = job.currentAttempt().nodeId();

        // 第 1 次丢失：attempt#1 -> 重试
        h.nodeService.markNodeLost(n1st, "loss-1");
        h.schedulerService.tick();
        assertEquals(JobState.RUNNING, job.state());
        assertEquals(2, job.attemptCount());

        // 第 2 次丢失：attempt#2 后 attemptCount=2 > maxRetries=1 -> FAILED
        String n2nd = job.currentAttempt().nodeId();
        h.nodeService.markNodeLost(n2nd, "loss-2");
        h.schedulerService.tick();
        assertEquals(JobState.FAILED, job.state());
        assertEquals(TerminationCause.MAX_RETRIES_EXHAUSTED, job.terminationCause());
        assertEquals(0, totalUsed(h), "终止后资源必须全部释放");
        log.info("[判定依据] 两次丢失后 job 终态=FAILED，cause={}，全集群占用=0",
                job.terminationCause());
    }

    @Test
    void heartbeatTimeoutTriggersSameConvergence(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.props.setHeartbeatTimeoutMillis(500);
        h.nodeService.registerNode("n1", 4, 8192, 0);
        h.nodeService.registerNode("n2", 4, 8192, 0);

        Job job = h.jobService.submitJob("j1", "j1",
                new ResourceRequest(4, 1024, 0, 60), null, 1);
        h.schedulerService.tick();
        String node = job.currentAttempt().nodeId();

        // 推进时钟超过心跳超时，但不主动 markNodeLost -> tick 内判定超时并重试
        h.clock.advance(600);
        int started = h.schedulerService.tick();
        assertEquals(1, started);
        assertEquals(JobState.RUNNING, job.state());
        assertEquals(2, job.attemptCount());
        log.info("[判定依据] 静默600ms > 阈值500ms，心跳超时触发重试 attempt#2，节点 {} 用量归零",
                node);
        assertEquals(0, h.state.nodes().get(node).usedCores());

        // 持续心跳保活：推进时钟不超过阈值，作业不受影响
        for (int i = 0; i < 5; i++) {
            h.clock.advance(400);
            h.jobService.heartbeat(job.id(), job.currentAttempt().attemptId());
            h.schedulerService.tick();
        }
        assertEquals(JobState.RUNNING, job.state());
        assertEquals(2, job.attemptCount(), "心跳正常时不应产生新尝试");
        log.info("[判定依据] 每400ms心跳一次（阈值500ms），5 轮后作业仍为 attempt#2 RUNNING");
    }

    @Test
    void duplicateNodeLostNotificationIsIdempotent(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("events.log"));
        h.nodeService.registerNode("n1", 4, 8192, 0);
        Job job = h.jobService.submitJob("j1", "j1",
                new ResourceRequest(4, 1024, 0, 60), null, 0);
        h.schedulerService.tick();

        int first = h.nodeService.markNodeLost("n1", "loss");
        int second = h.nodeService.markNodeLost("n1", "loss-duplicate");
        assertEquals(1, first);
        assertEquals(0, second, "重复掉线通知不得重复收敛/重复释放");
        assertEquals(0, totalUsed(h));
        log.info("[判定依据] 首次掉线收敛1个尝试，重复通知返回0且资源仍为0");
    }

    private int totalUsed(TestHarness h) {
        int sum = 0;
        for (ComputeNode n : h.state.nodes().values()) {
            sum += n.usedCores();
        }
        return sum;
    }
}

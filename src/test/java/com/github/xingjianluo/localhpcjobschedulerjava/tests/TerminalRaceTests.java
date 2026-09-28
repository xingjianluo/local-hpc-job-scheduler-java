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
 * 终态唯一性与上报幂等：
 * - 取消与完成无论以什么顺序到达，都收敛到唯一终态，资源只释放一次；
 * - 重复完成、重复心跳、旧 attempt 的迟到上报不得改变状态或重复释放资源。
 */
class TerminalRaceTests {

    private static final Logger log = LoggerFactory.getLogger(TerminalRaceTests.class);

    private TestHarness runningJob(Path dir, String name) {
        TestHarness h = TestHarness.create(dir.resolve("events-" + name + ".log"));
        h.nodeService.registerNode("n1", 4, 8192, 0);
        return h;
    }

    @Test
    void completeThenCancelConvergesToCompleted(@TempDir Path dir) {
        TestHarness h = runningJob(dir, "cc");
        Job job = h.jobService.submitJob("j", "j", rr(), null, null);
        h.schedulerService.tick();
        String attempt = job.currentAttempt().attemptId();

        h.jobService.complete(job.id(), attempt);
        h.jobService.cancel(job.id(), "late-cancel"); // 迟到取消

        assertEquals(JobState.COMPLETED, job.state(), "先完成后取消须保持 COMPLETED");
        assertEquals(TerminationCause.COMPLETED_BY_WORKER, job.terminationCause());
        assertEquals(0, usedCores(h));
        log.info("[判定依据] complete 先于 cancel：终态=COMPLETED，成因保持 {}，资源=0",
                job.terminationCause());
    }

    @Test
    void cancelThenCompleteConvergesToCancelled(@TempDir Path dir) {
        TestHarness h = runningJob(dir, "pc");
        Job job = h.jobService.submitJob("j", "j", rr(), null, null);
        h.schedulerService.tick();
        String attempt = job.currentAttempt().attemptId();

        h.jobService.cancel(job.id(), "user-cancel");
        h.jobService.complete(job.id(), attempt); // 完成上报在取消之后到达

        assertEquals(JobState.CANCELLED, job.state(), "先取消后完成须保持 CANCELLED，状态不倒退");
        assertEquals(TerminationCause.CANCELLED_BY_CLIENT, job.terminationCause());
        assertEquals(0, usedCores(h), "资源在取消路径只释放一次，迟到完成不重复释放");
        log.info("[判定依据] cancel 先于 complete：终态=CANCELLED，迟到完成不使状态倒退，资源=0");
    }

    @Test
    void duplicateCompleteReleasesResourcesExactlyOnce(@TempDir Path dir) {
        TestHarness h = runningJob(dir, "dc");
        Job job = h.jobService.submitJob("j", "j", rr(), null, null);
        h.schedulerService.tick();
        String nodeId = job.currentAttempt().nodeId();
        String attempt = job.currentAttempt().attemptId();
        assertEquals(4, h.state.nodes().get(nodeId).usedCores());

        h.jobService.complete(job.id(), attempt);
        // 模拟工作进程重试导致的重复完成上报
        for (int i = 0; i < 3; i++) {
            h.jobService.complete(job.id(), attempt);
        }
        assertEquals(JobState.COMPLETED, job.state());
        assertEquals(0, h.state.nodes().get(nodeId).usedCores());
        assertEquals(0, h.state.attemptIndex().size(), "attempt 索引只清理一次");
        log.info("[判定依据] 1次正常+3次重复完成上报：终态 COMPLETED，节点用量=0，attempt索引清空");
    }

    @Test
    void duplicateAndStaleHeartbeatsAreIgnored(@TempDir Path dir) {
        TestHarness h = runningJob(dir, "hb");
        h.props.setHeartbeatTimeoutMillis(500);
        Job job = h.jobService.submitJob("j", "j", rr(), null, null);
        h.schedulerService.tick();
        String attempt = job.currentAttempt().attemptId();

        // 重复心跳全部幂等
        for (int i = 0; i < 5; i++) {
            h.jobService.heartbeat(job.id(), attempt);
        }
        assertEquals(JobState.RUNNING, job.state());

        // 旧 attempt / 不存在 attempt / 终态作业的心跳均被忽略，不改状态
        h.jobService.heartbeat(job.id(), "attempt-old-stale");
        h.jobService.complete(job.id(), attempt);
        h.jobService.heartbeat(job.id(), attempt);
        assertEquals(JobState.COMPLETED, job.state());
        log.info("[判定依据] 重复心跳5次、旧attempt心跳、终态后心跳均被幂等忽略，状态轨迹正确");
    }

    @Test
    void staleCompletionFromOldAttemptDoesNotRevertState(@TempDir Path dir) {
        TestHarness h = runningJob(dir, "sc");
        h.nodeService.registerNode("n2", 4, 8192, 0);
        Job job = h.jobService.submitJob("j", "j", rr(), null, 1);
        h.schedulerService.tick();
        String firstAttempt = job.currentAttempt().attemptId();
        String firstNode = job.currentAttempt().nodeId();

        // 第一次尝试掉线 -> 第二次尝试在另一节点运行
        h.nodeService.markNodeLost(firstNode, "loss");
        h.schedulerService.tick();
        assertEquals(2, job.attemptCount());
        String secondAttempt = job.currentAttempt().attemptId();

        // 旧工作进程"复活"后迟到的完成上报：必须被忽略
        h.jobService.complete(job.id(), firstAttempt);
        assertEquals(JobState.RUNNING, job.state(), "旧 attempt 的迟到完成不得影响当前运行");
        assertEquals(secondAttempt, job.currentAttempt().attemptId());

        // 当前 attempt 正常完成
        h.jobService.complete(job.id(), secondAttempt);
        assertEquals(JobState.COMPLETED, job.state());
        assertEquals(0, usedCores(h));
        log.info("[判定依据] 旧attempt迟到完成被忽略，当前attempt#2正常完成，全集群资源=0");
    }

    @Test
    void cancelPendingJobNeverOccupiesResources(@TempDir Path dir) {
        TestHarness h = runningJob(dir, "cp");
        Job big = h.jobService.submitJob("big", "big",
                new ResourceRequest(4, 1024, 0, 60), null, null);
        Job small = h.jobService.submitJob("small", "small",
                new ResourceRequest(1, 1024, 0, 60), null, null);
        h.schedulerService.tick();
        // big 占满 4 核，small 排队
        assertEquals(JobState.RUNNING, big.state());
        assertEquals(JobState.PENDING, small.state());

        h.jobService.cancel(small.id(), "give-up");
        assertEquals(JobState.CANCELLED, small.state());
        assertEquals(4, usedCores(h), "取消排队作业不影响在跑作业占用");
        // 重复取消幂等
        h.jobService.cancel(small.id(), "again");
        assertEquals(JobState.CANCELLED, small.state());
        assertEquals(TerminationCause.CANCELLED_BY_CLIENT, small.terminationCause());
        log.info("[判定依据] PENDING 作业取消即 CANCELLED（从未占资源），重复取消幂等");
    }

    private ResourceRequest rr() {
        return new ResourceRequest(4, 1024, 0, 60);
    }

    private int usedCores(TestHarness h) {
        int sum = 0;
        for (ComputeNode n : h.state.nodes().values()) {
            sum += n.usedCores();
        }
        return sum;
    }
}

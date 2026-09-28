package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.model.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.NodeState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerEngine;
import com.github.xingjianluo.localhpcjobschedulerjava.tests.TestSupport.Harness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 异常收敛：节点失联、心跳超时、整组重试、重试不重复计资源、重试耗尽失败、节点恢复。
 */
class FailureRetryTest {

    private static final Logger log = LoggerFactory.getLogger(FailureRetryTest.class);

    @Test
    void nodeLossMovesRunningArrayToRetryThenReschedulesOnRecovery() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(8, 8192, 0));
        e.registerNode("n2", new Resources(8, 8192, 0));

        e.submitJob("arr", "阵列", 3, new Resources(4, 1, 0), 1000, 0, null, null);
        assertEquals(JobState.RUNNING, e.getJob("arr").state());
        String nodeOfTask0 = e.getJob("arr").tasks().get(0).nodeId();
        log.info("[关键输入] 阵列 3×4 核已派发，任务#0 在 {}；现将其置为失联", nodeOfTask0);

        // 模拟节点失联：整个作业整组进入 RETRY_WAIT（阵列语义：同组任务一并回收）
        e.markNodeDown(nodeOfTask0, true);
        assertEquals(JobState.RETRY_WAIT, e.getJob("arr").state());
        assertEquals(NodeState.LOST, e.getNode(nodeOfTask0).state());
        for (var t : e.getJob("arr").tasks()) {
            assertEquals(JobState.RETRY_WAIT, t.state(), "整组失败转移：所有任务一同等待重试");
            assertEquals(null, t.nodeId(), "失败任务应解除节点绑定");
        }
        // 失联节点用量清零；仍在线节点的用量也因整组回收归零
        long totalUsed = e.nodes().stream()
                .filter(n -> n.state() == NodeState.UP)
                .mapToLong(n -> n.used().cpus()).sum();
        log.info("[判定依据] 失联后在线节点已用核数合计={}（期望 0：整组回收，无幽灵占用）", totalUsed);
        assertEquals(0, totalUsed);

        // 退避 50ms 未结束前仍排队等待
        h.advance(40);
        e.detectTimeouts();
        assertEquals(JobState.RETRY_WAIT, e.getJob("arr").state());

        // 退避结束：在剩余一个在线节点上放不下 3×4（只有 8 核），继续排队
        h.advance(20);
        e.schedule();
        assertEquals(JobState.QUEUED, e.getJob("arr").state());

        // 失联节点恢复 -> 整组重新派发，attemptNo 增加但资源只计一次
        e.recoverNode(nodeOfTask0);
        assertEquals(JobState.RUNNING, e.getJob("arr").state());
        long usedAfterRetry = e.nodes().stream().mapToLong(n -> n.used().cpus()).sum();
        log.info("[判定依据] 重试后阵列重新 RUNNING，attempt#0={}，全集群已用核数={}（期望 12，未重复计入）",
                e.getJob("arr").tasks().get(0).attemptNo(), usedAfterRetry);
        assertEquals(2, e.getJob("arr").tasks().get(0).attemptNo(), "重试后尝试次数 +1");
        assertEquals(12, usedAfterRetry);
    }

    @Test
    void heartbeatTimeoutTriggersGangRetryAndResourcesAreNotDoubleCounted() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(16, 16384, 0));

        e.submitJob("arr", "阵列", 2, new Resources(4, 1, 0), 1000, 0, null, null);
        assertEquals(JobState.RUNNING, e.getJob("arr").state());

        // 任务 #0 心跳超时（节点仍在线）：整组回收并重试
        h.advance(101);
        int affected = e.detectTimeouts();
        log.info("[关键输入] 心跳超时 101ms > 100ms；受影响作业数={}", affected);
        assertEquals(1, affected);
        assertEquals(JobState.RETRY_WAIT, e.getJob("arr").state());
        assertEquals(0, e.getNode("n1").used().cpus(), "超时回收后节点用量必须归零，重试不重复计");

        h.advance(60);
        e.schedule();
        assertEquals(JobState.RUNNING, e.getJob("arr").state());
        assertEquals(8, e.getNode("n1").used().cpus(), "重新整组派发后用量=2×4=8，而非叠加成 16");
        // 迟到/重复心跳来自旧尝试，幂等忽略，不污染状态
        e.taskHeartbeat("arr.0");
        h.advance(5);
        e.taskHeartbeat("arr.0");
        assertEquals(8, e.getNode("n1").used().cpus());
        log.info("[判定依据] 重试派发后重复心跳被接受（仍为 RUNNING 任务），用量稳定=8");
    }

    @Test
    void retriesExhaustedFailTheWholeJob() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(4, 4096, 0));
        // maxRetries=1：首次 attempt=1，失败一次后还可重试（attempt=2），再失败即耗尽
        e.submitJob("j", "脆弱作业", 1, new Resources(2, 1, 0), 1000, 0, 1, 1L);
        assertEquals(JobState.RUNNING, e.getJob("j").state());

        h.advance(101);
        e.detectTimeouts();
        assertEquals(JobState.RETRY_WAIT, e.getJob("j").state());
        h.advance(2);
        e.schedule();
        assertEquals(JobState.RUNNING, e.getJob("j").state());
        assertEquals(2, e.getJob("j").tasks().get(0).attemptNo());

        h.advance(101);
        e.detectTimeouts();
        log.info("[判定依据] 第二次心跳超时（attempt=2，maxRetries=1）-> 作业终态={}，原因={}",
                e.getJob("j").state(), e.getJob("j").reasonDetail());
        assertEquals(JobState.FAILED, e.getJob("j").state());
        assertEquals(JobState.FAILED, e.getJob("j").tasks().get(0).state());
        assertEquals(0, e.getNode("n1").used().cpus(), "失败终态不残留资源");
        assertTrue(e.getJob("j").reasonDetail().contains("重试次数已用尽"));
    }

    @Test
    void lateCompleteFromLostAttemptIsRejectedAndDoesNotCorruptState() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(8, 8192, 0));
        e.registerNode("n2", new Resources(8, 8192, 0));
        e.submitJob("arr", "阵列", 2, new Resources(4, 1, 0), 1000, 0, null, null);
        String nid = e.getJob("arr").tasks().get(0).nodeId();
        e.markNodeDown(nid, true);
        assertEquals(JobState.RETRY_WAIT, e.getJob("arr").state());

        // 旧尝试“复活”并上报完成：必须被拒绝（任务已不处于 RUNNING），状态不被污染
        var ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.github.xingjianluo.localhpcjobschedulerjava.exception.ConflictException.class,
                () -> e.taskComplete("arr.0"));
        log.info("[判定依据] 失联旧尝试的迟到完成上报被拒绝：{}", ex.getMessage());
        assertEquals(JobState.RETRY_WAIT, e.getJob("arr").state());
    }

    @Test
    void offlineNodeDoesNotReceiveAllocationsButJobsStayExplainablyQueued() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(8, 8192, 0));
        e.markNodeDown("n1", false); // 主动下线
        var out = e.submitJob("j", "作业", 1, new Resources(2, 1, 0), 1000, 0, null, null);
        // 容量预检按理论容量通过，但调度时无在线节点
        assertEquals(JobState.QUEUED, out.job().state());
        assertEquals(com.github.xingjianluo.localhpcjobschedulerjava.model.QueueReason.NO_ONLINE_NODE,
                e.getJob("j").queueReason());
        log.info("[判定依据] 全部节点下线时作业排队原因={}，{}",
                e.getJob("j").queueReason(), e.getJob("j").reasonDetail());
        e.recoverNode("n1");
        assertEquals(JobState.RUNNING, e.getJob("j").state());
        assertNotEquals(null, e.getJob("j").tasks().get(0).nodeId());
    }
}

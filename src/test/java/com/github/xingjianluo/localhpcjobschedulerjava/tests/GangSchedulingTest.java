package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.model.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.QueueReason;
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
 * 整组调度：一次性满足、部分满足时零预留、优先级顺序、老化防饥饿、容量不超分。
 */
class GangSchedulingTest {

    private static final Logger log = LoggerFactory.getLogger(GangSchedulingTest.class);

    @Test
    void arrayDispatchedAllAtOnceAndBindsDistinctOrSharedNodesCorrectly() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(8, 8192, 2));
        e.registerNode("n2", new Resources(8, 8192, 2));

        // 阵列 4 任务各 4 核：单节点可放 2 个，共 16 核刚好放满
        var out = e.submitJob("arr", "阵列", 4, new Resources(4, 1, 1), 1000, 0, null, null);
        assertEquals(SchedulerEngine.SubmitOutcome.Kind.ACCEPTED, out.kind());
        assertEquals(JobState.RUNNING, e.getJob("arr").state());
        for (var t : e.getJob("arr").tasks()) {
            assertEquals(JobState.RUNNING, t.state(), "整组派发后每个任务都应 RUNNING");
            assertNotEquals(null, t.nodeId());
        }
        long n1Used = e.getNode("n1").used().cpus();
        long n2Used = e.getNode("n2").used().cpus();
        log.info("[判定依据] 4×4 核整组派发：n1 已用 {}，n2 已用 {}（合计必须=16，且均不超容量 8）",
                n1Used, n2Used);
        assertEquals(16, n1Used + n2Used);
        assertTrue(n1Used <= 8 && n2Used <= 8, "任何节点不得超额分配");
        assertEquals(4, e.getNode("n1").used().gpus() + e.getNode("n2").used().gpus());
    }

    @Test
    void higherPriorityGoesFirstAndLowerOneWaitsWithExplicitReason() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        // 先占用 4 核（集群 6 核），让后续两个 4 核作业都处于排队状态
        e.registerNode("n1", new Resources(6, 8192, 0));
        e.submitJob("keeper", "占用者", 1, new Resources(4, 1, 0), 1000, 0, null, null);

        var low = e.submitJob("low", "低优先级", 1, new Resources(4, 1, 0), 1000, 1, null, null);
        var high = e.submitJob("high", "高优先级", 1, new Resources(4, 1, 0), 1000, 10, null, null);
        assertEquals(JobState.QUEUED, e.getJob("low").state());
        assertEquals(JobState.QUEUED, e.getJob("high").state());
        log.info("[关键输入] 剩余 2 核；low(prio=1,seq在前)、high(prio=10) 同时排队");

        // keeper 完成释放 4 核，触发调度：两个排队作业只有一个能整组得到满足
        e.taskComplete("keeper.0");
        log.info("[判定依据] 释放后 low={}({}), high={}({})",
                e.getJob("low").state(), e.getJob("low").queueReason(),
                e.getJob("high").state(), e.getJob("high").queueReason());
        assertEquals(JobState.RUNNING, e.getJob("high").state(), "高优先级应先被整组调度");
        assertEquals(JobState.QUEUED, e.getJob("low").state(), "低优先级继续排队");
        assertEquals(QueueReason.INSUFFICIENT_FOR_GANG, e.getJob("low").queueReason());
        assertEquals(4, e.getNode("n1").used().cpus(), "只分配给一个作业，不超分");
    }

    @Test
    void agingPreventsStarvationOfOldLowPriorityJob() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        // 6 核节点：holder 占 2 核后只剩 4 核；两个 5 核作业都得排队；
        // holder 释放后有 6 核，两个 5 核作业里只有一个能整组得到满足。
        e.registerNode("n1", new Resources(6, 8192, 0));

        e.submitJob("holder", "常驻", 1, new Resources(2, 1, 0), 1000, 100, null, null);
        e.submitJob("old", "老作业", 1, new Resources(5, 1, 0), 1000, 1, null, null);
        assertEquals(JobState.QUEUED, e.getJob("old").state());

        // 老作业等待 9500ms 后（有效优先级 = 1+9 = 10），新来静态优先级 10 的作业（老化 0）。
        h.advance(9500);
        e.submitJob("new-high", "新的同优先级作业", 1, new Resources(5, 1, 0), 1000, 10, null, null);
        e.schedule(); // 只剩 4 核，无人可派发

        e.taskComplete("holder.0"); // 释放 2 核后出现 6 核空闲
        long oldEffective = 1 + (h.clock().get() - e.getJob("old").createdAtMs()) / 1000;
        long newEffective = 10 + (h.clock().get() - e.getJob("new-high").createdAtMs()) / 1000;
        log.info("[判定依据] 释放后 old={}（有效优先级 {}），new-high={}（有效优先级 {}），6 核只够一个 5 核作业",
                e.getJob("old").state(), oldEffective,
                e.getJob("new-high").state(), newEffective);
        assertEquals(oldEffective, newEffective, "测试设计：两者有效优先级相同，应由提交次序破平");
        assertEquals(JobState.RUNNING, e.getJob("old").state(),
                "同有效优先级时更老的作业不得被后来者长期插队，防止饥饿");
        assertEquals(JobState.QUEUED, e.getJob("new-high").state());

        // old 完成后 new-high 才获得资源
        e.taskComplete("old.0");
        assertEquals(JobState.RUNNING, e.getJob("new-high").state());
        log.info("[判定依据] old 完成后 new-high={}，节点已用核数={}（不超分）",
                e.getJob("new-high").state(), e.getNode("n1").used().cpus());
    }

    @Test
    void schedulingIsDeterministicAcrossRuns() throws Exception {
        // 同样的输入重复建两套集群，派发结果（任务->节点）必须一致
        java.util.List<java.util.Map<String, String>> layouts = new java.util.ArrayList<>();
        for (int round = 0; round < 3; round++) {
            Harness h = TestSupport.engine();
            SchedulerEngine e = h.engine();
            e.registerNode("node-a", new Resources(4, 4096, 0));
            e.registerNode("node-b", new Resources(4, 4096, 0));
            e.submitJob("j1", "j1", 2, new Resources(2, 1, 0), 1000, 0, null, null);
            e.submitJob("j2", "j2", 2, new Resources(2, 1, 0), 1000, 0, null, null);
            java.util.Map<String, String> layout = new java.util.TreeMap<>();
            e.getJob("j1").tasks().forEach(t -> layout.put(t.id(), t.nodeId()));
            e.getJob("j2").tasks().forEach(t -> layout.put(t.id(), t.nodeId()));
            layouts.add(layout);
        }
        log.info("[判定依据] 三轮相同输入的放置布局：{}", layouts);
        assertEquals(layouts.get(0), layouts.get(1));
        assertEquals(layouts.get(1), layouts.get(2));
    }
}

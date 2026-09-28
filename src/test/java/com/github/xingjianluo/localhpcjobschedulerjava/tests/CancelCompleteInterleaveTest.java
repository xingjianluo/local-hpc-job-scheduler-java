package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.exception.ConflictException;
import com.github.xingjianluo.localhpcjobschedulerjava.model.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerEngine;
import com.github.xingjianluo.localhpcjobschedulerjava.tests.TestSupport.Harness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 取消与完成严格交错（确定性顺序，而非随机并发）：
 * 两个方向的“迟到方”都不得重复释放资源或导致状态倒退。
 */
class CancelCompleteInterleaveTest {

    private static final Logger log = LoggerFactory.getLogger(CancelCompleteInterleaveTest.class);

    @Test
    void cancelWinsThenLateCompleteMustNotReleaseAgain() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(8, 8192, 0));
        e.submitJob("j", "作业", 1, new Resources(3, 1, 0), 1000, 0, null, null);
        assertEquals(3, e.getNode("n1").used().cpus());

        e.cancelJob("j"); // 取消先到：释放 3，任务 CANCELLED
        assertEquals(0, e.getNode("n1").used().cpus());
        assertEquals(JobState.CANCELLED, e.getJob("j").state());

        ConflictException ex = assertThrows(ConflictException.class,
                () -> e.taskComplete("j.0"),
                "取消已胜出，迟到的完成必须被拒绝");
        log.info("[判定依据] 取消胜出后迟到完成被拒：{}", ex.getMessage());
        assertEquals(0, e.getNode("n1").used().cpus(), "迟到完成不得二次释放（用量不得为负或反弹）");
        assertEquals(JobState.CANCELLED, e.getJob("j").state(), "终态不得倒退");
        assertEquals(JobState.CANCELLED, e.getJob("j").tasks().get(0).state());
    }

    @Test
    void completeWinsThenLateCancelMustConflictAndKeepCompleted() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(8, 8192, 0));
        e.submitJob("j", "作业", 1, new Resources(3, 1, 0), 1000, 0, null, null);

        e.taskComplete("j.0"); // 完成先到：释放一次
        assertEquals(0, e.getNode("n1").used().cpus());

        ConflictException ex = assertThrows(ConflictException.class, () -> e.cancelJob("j"));
        log.info("[判定依据] 完成胜出后迟到取消被拒：{}", ex.getMessage());
        assertEquals(0, e.getNode("n1").used().cpus());
        assertEquals(JobState.COMPLETED, e.getJob("j").state(), "终态收敛为 COMPLETED");
    }

    @Test
    void arrayMixedTerminalStatesReleaseExactlyOncePerTask() throws Exception {
        Harness h = TestSupport.engine();
        SchedulerEngine e = h.engine();
        e.registerNode("n1", new Resources(16, 16384, 0));
        e.submitJob("arr", "阵列", 3, new Resources(4, 1, 0), 1000, 0, null, null);
        assertEquals(12, e.getNode("n1").used().cpus());

        // 任务0完成（释放4）；随后取消整个阵列（任务1、2取消，各释放4）
        e.taskComplete("arr.0");
        assertEquals(8, e.getNode("n1").used().cpus());
        // 对已完成任务再发完成：幂等，无释放
        e.taskComplete("arr.0");
        assertEquals(8, e.getNode("n1").used().cpus());

        e.cancelJob("arr");
        log.info("[判定依据] 1 完成 + 2 取消的混合终态：{}，节点已用={}",
                e.getJob("arr").tasks().stream().map(t -> t.id() + "=" + t.state()).toList(),
                e.getNode("n1").used().cpus());
        assertEquals(0, e.getNode("n1").used().cpus(), "每个任务恰好释放一次");
        // 混合终态：有完成有取消，作业按 FAILED>CANCELLED>COMPLETED 规则聚合为 CANCELLED
        assertEquals(JobState.CANCELLED, e.getJob("arr").state());

        // 迟到的完成请求打在已取消任务上：拒绝且无释放
        assertThrows(ConflictException.class, () -> e.taskComplete("arr.1"));
        assertEquals(0, e.getNode("n1").used().cpus());
    }
}

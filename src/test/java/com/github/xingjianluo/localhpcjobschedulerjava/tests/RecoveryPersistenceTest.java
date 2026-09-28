package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.localhpcjobschedulerjava.config.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.model.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerEngine;
import com.github.xingjianluo.localhpcjobschedulerjava.store.SnapshotStore;
import com.github.xingjianluo.localhpcjobschedulerjava.tests.TestSupport.Harness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 进程重启恢复：从本地快照重建节点/作业；曾在运行的任务统一回到队列重新调度，
 * 节点上不残留幽灵用量；终态作业保持终态。
 */
class RecoveryPersistenceTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryPersistenceTest.class);

    private SchedulerEngine restart(Path file, SchedulerProperties props) {
        SnapshotStore store = new SnapshotStore(file, new ObjectMapper());
        SchedulerEngine engine = new SchedulerEngine(store, props);
        java.util.concurrent.atomic.AtomicLong clock =
                new java.util.concurrent.atomic.AtomicLong(2_000_000L);
        engine.setClock(clock::get);
        return engine;
    }

    @Test
    void restartWithSharedSnapshotFile() throws Exception {
        Path file = Path.of("target/test-data/shared-" + java.util.UUID.randomUUID() + ".json");
        Files.createDirectories(file.getParent());
        SchedulerProperties props = new SchedulerProperties(
                file.toString(), 100, 200, 9_999_999L, 1_000L, 1, 50L, false, false);
        SnapshotStore store1 = new SnapshotStore(file, new ObjectMapper());
        java.util.concurrent.atomic.AtomicLong clock =
                new java.util.concurrent.atomic.AtomicLong(1_000_000L);
        SchedulerEngine e1 = new SchedulerEngine(store1, props);
        e1.setClock(clock::get);

        e1.registerNode("n1", new Resources(8, 8192, 0));
        e1.registerNode("n2", new Resources(8, 8192, 0));
        // 阵列两个 6 核任务：每个节点恰好放一个，整组占满两台
        e1.submitJob("arr", "运行阵列", 2, new Resources(6, 1, 0), 1000, 0, null, null);
        e1.submitJob("done", "完成作业", 1, new Resources(2, 1, 0), 1000, 0, null, null);
        e1.taskComplete("done.0");
        // 每台仅剩 2 核，8 核作业整组放不下，保持排队
        e1.submitJob("q", "排队", 1, new Resources(8, 1, 0), 1000, 0, null, null);
        assertEquals(JobState.RUNNING, e1.getJob("arr").state());
        assertEquals(JobState.QUEUED, e1.getJob("q").state());
        long usageBeforeCrash = e1.getNode("n1").used().cpus() + e1.getNode("n2").used().cpus();
        log.info("[关键输入] “崩溃前”：arr RUNNING（2×6 核，各占一台），q QUEUED，done COMPLETED；"
                + "已用核数合计={}", usageBeforeCrash);

        // ===== 模拟进程重启：新建引擎，构造器自动从快照恢复 =====
        SchedulerEngine e2 = restart(file, props);

        assertEquals(2, e2.nodes().size(), "节点表恢复");
        assertEquals(3, e2.jobs().size(), "作业表恢复");
        assertEquals(JobState.COMPLETED, e2.getJob("done").state(), "终态作业保持终态");
        assertEquals(JobState.QUEUED, e2.getJob("arr").state(),
                "曾运行任务重启后回到队列重新调度，尝试次数不因恢复而增加");
        assertEquals(JobState.QUEUED, e2.getJob("q").state());
        for (var t : e2.getJob("arr").tasks()) {
            assertEquals(1, t.attemptNo(),
                    "恢复不额外增加尝试次数：崩溃前真实启动过 1 次，仍为 1（不白送重试预算）");
        }
        long ghostUsage = e2.nodes().stream()
                .filter(n -> n.state().toString().equals("UP"))
                .mapToLong(n -> n.used().cpus()).sum();
        log.info("[判定依据] 重启后在线节点幽灵用量合计={}（期望 0）；随后重新调度……", ghostUsage);
        assertEquals(0, ghostUsage, "节点上不得残留已死任务的用量");

        // 重新调度后阵列再次整组运行，用量与崩溃前一致而非翻倍
        e2.schedule();
        assertEquals(JobState.RUNNING, e2.getJob("arr").state());
        long usageAfterResched = e2.getNode("n1").used().cpus() + e2.getNode("n2").used().cpus();
        assertEquals(12, usageAfterResched, "重新调度只计一次用量（2×6），不叠加旧占用");
        log.info("[判定依据] 恢复后重新调度 arr=RUNNING，已用核数={}（期望 12）", usageAfterResched);
    }
}

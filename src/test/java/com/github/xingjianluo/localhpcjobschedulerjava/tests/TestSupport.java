package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.localhpcjobschedulerjava.config.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerEngine;
import com.github.xingjianluo.localhpcjobschedulerjava.store.SnapshotStore;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/** 测试公用工厂：临时快照文件 + 可控时钟 + 短超时参数。 */
final class TestSupport {

    private TestSupport() {
    }

    record Harness(SchedulerEngine engine, AtomicLong clock) {
        void advance(long deltaMs) {
            clock.addAndGet(deltaMs);
        }
    }

    static Harness engine() throws Exception {
        Path tmp = Path.of("target", "test-data",
                "snapshot-" + java.util.UUID.randomUUID() + ".json");
        SnapshotStore store = new SnapshotStore(tmp, new ObjectMapper());
        // 心跳超时 100ms，节点超时 200ms，老化 1000ms，默认重试 1 次、退避 50ms
        SchedulerProperties props = new SchedulerProperties(
                tmp.toString(), 100, 200, 9_999_999L, 1_000L, 1, 50L, false, false);
        AtomicLong clock = new AtomicLong(1_000_000L);
        SchedulerEngine engine = new SchedulerEngine(store, props);
        engine.setClock(clock::get);
        return new Harness(engine, clock);
    }
}

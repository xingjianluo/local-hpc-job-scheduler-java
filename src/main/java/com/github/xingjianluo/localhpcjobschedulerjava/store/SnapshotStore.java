package com.github.xingjianluo.localhpcjobschedulerjava.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 本地快照存储：单文件 JSON。
 *
 * <p>写入流程：先写同目录临时文件并 {@code fsync} 风格 flush，再 {@link Files#move}
 * 原子替换目标文件，确保进程崩溃后恢复时不会读到半截 JSON。</p>
 *
 * <p>该存储只用于“进程重启后把内存状态恢复到一致点”，不模拟真实数据库；
 * 恢复时所有曾处于 RUNNING 的任务槽统一回到队列重新调度（见 SchedulerEngine 恢复逻辑）。</p>
 */
public class SnapshotStore {

    private static final Logger log = LoggerFactory.getLogger(SnapshotStore.class);

    private final Path file;
    private final ObjectMapper mapper;

    public SnapshotStore(Path file, ObjectMapper mapper) {
        this.file = file;
        // 领域对象是普通可变类（私有字段 + 派生 getter），快照专用 mapper 只看字段、
        // 忽略全部 getter/setter/is，避免 online/available 等派生属性混入快照。
        this.mapper = mapper.copy()
                .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    public synchronized void save(PersistentState state) {
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), state);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            log.debug("快照已落盘：{}（节点 {} 个，作业 {} 个）",
                    file, state.nodes().size(), state.jobs().size());
        } catch (IOException e) {
            throw new UncheckedIOException("快照写入失败: " + file, e);
        }
    }

    /** 不存在快照文件时返回 null（首次启动）。 */
    public synchronized PersistentState load() {
        if (!Files.exists(file)) {
            log.info("未发现快照文件 {}，按全新集群启动", file);
            return null;
        }
        try {
            PersistentState state = mapper.readValue(file.toFile(), PersistentState.class);
            log.info("已从快照 {} 恢复：节点 {} 个，作业 {} 个",
                    file, state.nodes().size(), state.jobs().size());
            return state;
        } catch (IOException e) {
            throw new UncheckedIOException("快照读取失败: " + file, e);
        }
    }

    public synchronized void clear() {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException("快照删除失败: " + file, e);
        }
    }
}

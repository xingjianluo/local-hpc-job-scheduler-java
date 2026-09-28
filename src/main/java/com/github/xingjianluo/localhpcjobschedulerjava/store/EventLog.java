package com.github.xingjianluo.localhpcjobschedulerjava.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 本地追加式事件日志：每行一个 JSON 对象，只追加不修改。
 * 进程重启后通过 {@link #replay()} 重放恢复一致状态。
 * 不依赖任何外部服务或真实凭据。
 */
public class EventLog {

    private static final Logger log = LoggerFactory.getLogger(EventLog.class);

    private final Path path;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object fileLock = new Object();
    private volatile long lastSeq = 0;

    public EventLog(String path) {
        this.path = Path.of(path);
    }

    public String path() {
        return path.toString();
    }

    /**
     * 追加一条事件并强制落盘；返回带单调序号的事件。
     * 序号在文件内单调递增，重放后继续从最大序号 +1 开始。
     */
    public SchedulerEvent append(String type, long millis, String jobId,
                                 String nodeId, String attemptId, List<String> payload) {
        synchronized (fileLock) {
            try {
                ensureParent();
                SchedulerEvent event = new SchedulerEvent(
                        ++lastSeq, type, millis, jobId, nodeId, attemptId,
                        payload == null ? List.of() : List.copyOf(payload));
                String line = mapper.writeValueAsString(Map.of(
                        "seq", event.seq(),
                        "type", event.type(),
                        "millis", event.millis(),
                        "jobId", event.jobId() == null ? "" : event.jobId(),
                        "nodeId", event.nodeId() == null ? "" : event.nodeId(),
                        "attemptId", event.attemptId() == null ? "" : event.attemptId(),
                        "payload", event.payload()));
                Files.writeString(path, line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                return event;
            } catch (IOException e) {
                throw new UncheckedIOException("写入事件日志失败: " + path, e);
            }
        }
    }

    private void ensureParent() throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    /** 重放全部事件；文件不存在时返回空列表。重放后 lastSeq 对齐到最大序号。 */
    public List<SchedulerEvent> replay() {
        synchronized (fileLock) {
            if (!Files.exists(path)) {
                lastSeq = 0;
                return List.of();
            }
            List<SchedulerEvent> events = new ArrayList<>();
            long maxSeq = 0;
            try {
                for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    Map<String, Object> m = mapper.readValue(line, new TypeReference<Map<String, Object>>() {
                    });
                    long seq = ((Number) m.get("seq")).longValue();
                    @SuppressWarnings("unchecked")
                    List<String> payload = (List<String>) m.getOrDefault("payload", List.of());
                    events.add(new SchedulerEvent(
                            seq,
                            (String) m.get("type"),
                            ((Number) m.get("millis")).longValue(),
                            emptyToNull((String) m.get("jobId")),
                            emptyToNull((String) m.get("nodeId")),
                            emptyToNull((String) m.get("attemptId")),
                            List.copyOf(payload)));
                    maxSeq = Math.max(maxSeq, seq);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("读取事件日志失败: " + path, e);
            }
            lastSeq = maxSeq;
            log.info("重放事件日志 {} 完成，共 {} 条事件，末序号 {}", path, events.size(), maxSeq);
            return events;
        }
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    /** 日志是否为空（文件不存在或无内容）。 */
    public boolean isEmpty() {
        synchronized (fileLock) {
            try {
                return !Files.exists(path) || Files.size(path) == 0;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}

package com.github.xingjianluo.localhpcjobschedulerjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 调度器本地配置，全部通过 application.properties / 环境变量覆盖，无真实凭据。
 *
 * @param snapshotFile       本地 JSON 快照位置；为空表示不落盘（纯内存）
 * @param heartbeatTimeoutMs 工作进程心跳超过该时长未到达即判定超时
 * @param nodeTimeoutMs      节点心跳超过该时长未到达即判定失联
 * @param tickIntervalMs     后台巡检周期（超时检测 + 调度）
 * @param agingBoostPerMs    排队等待每经过该毫秒数，有效优先级 +1，用于防饥饿
 * @param defaultMaxRetries  提交时未显式指定的默认重试次数
 * @param defaultBackoffMs   默认失败重试退避
 * @param tickEnabled        是否启用后台周期巡检（单元测试关闭，改为手动触发）
 * @param demoCluster        启动时是否注册一组演示节点（仅在无快照节点时）
 */
@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        String snapshotFile,
        long heartbeatTimeoutMs,
        long nodeTimeoutMs,
        long tickIntervalMs,
        long agingBoostPerMs,
        int defaultMaxRetries,
        long defaultBackoffMs,
        boolean tickEnabled,
        boolean demoCluster) {

    public SchedulerProperties {
        if (heartbeatTimeoutMs <= 0) {
            heartbeatTimeoutMs = 15_000L;
        }
        if (nodeTimeoutMs <= 0) {
            nodeTimeoutMs = 20_000L;
        }
        if (tickIntervalMs <= 0) {
            tickIntervalMs = 2_000L;
        }
        if (agingBoostPerMs <= 0) {
            agingBoostPerMs = 5_000L;
        }
        if (defaultMaxRetries < 0) {
            defaultMaxRetries = 2;
        }
        if (defaultBackoffMs < 0) {
            defaultBackoffMs = 1_000L;
        }
    }

    /** 纯内存默认值。 */
    public static SchedulerProperties defaults() {
        return new SchedulerProperties(
                "data/scheduler-snapshot.json",
                15_000L, 20_000L, 2_000L, 5_000L, 2, 1_000L, true, false);
    }
}

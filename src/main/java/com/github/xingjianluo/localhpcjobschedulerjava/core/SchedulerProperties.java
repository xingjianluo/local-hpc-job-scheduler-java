package com.github.xingjianluo.localhpcjobschedulerjava.core;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 调度器配置（application.properties 前缀 scheduler）。
 */
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    /** 心跳超时（毫秒）。 */
    private long heartbeatTimeoutMillis = 5_000L;

    /** 后台巡检周期（毫秒），<=0 表示关闭后台巡检。 */
    private long tickIntervalMillis = 1_000L;

    /** 丢失后重试退避（毫秒）。 */
    private long retryBackoffMillis = 0L;

    /** 默认最大重试次数。 */
    private int defaultMaxRetries = 2;

    /** 排队等待多久后获得一次优先级提升（毫秒）。 */
    private long starvationBoostMillis = 10_000L;

    /** 优先级提升上限（避免低优先级饥饿）。 */
    private int maxBoost = 1_000_000;

    /** 事件日志文件路径。 */
    private String eventLogPath = "data/scheduler-events.log";

    /** 启动时是否播种演示节点（仅当事件日志为空）。 */
    private boolean seedDemoNodes = true;

    public long getHeartbeatTimeoutMillis() { return heartbeatTimeoutMillis; }
    public void setHeartbeatTimeoutMillis(long v) { this.heartbeatTimeoutMillis = v; }
    public long getTickIntervalMillis() { return tickIntervalMillis; }
    public void setTickIntervalMillis(long v) { this.tickIntervalMillis = v; }
    public long getRetryBackoffMillis() { return retryBackoffMillis; }
    public void setRetryBackoffMillis(long v) { this.retryBackoffMillis = v; }
    public int getDefaultMaxRetries() { return defaultMaxRetries; }
    public void setDefaultMaxRetries(int v) { this.defaultMaxRetries = v; }
    public long getStarvationBoostMillis() { return starvationBoostMillis; }
    public void setStarvationBoostMillis(long v) { this.starvationBoostMillis = v; }
    public int getMaxBoost() { return maxBoost; }
    public void setMaxBoost(int v) { this.maxBoost = v; }
    public String getEventLogPath() { return eventLogPath; }
    public void setEventLogPath(String v) { this.eventLogPath = v; }
    public boolean isSeedDemoNodes() { return seedDemoNodes; }
    public void setSeedDemoNodes(boolean v) { this.seedDemoNodes = v; }
}

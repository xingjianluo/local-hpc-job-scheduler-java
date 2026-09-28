package com.github.xingjianluo.localhpcjobschedulerjava.bootstrap;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.service.NodeService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.RecoveryService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 启动顺序（确定性）：
 * 1) 从本地事件日志恢复内存状态（空日志则跳过）；
 * 2) 仅当日志为空时播种演示节点（避免重复注册/重复占容量）；
 * 3) 启动后立即跑一轮调度，随后按固定周期后台巡检。
 */
@Component
@EnableScheduling
public class SchedulerBootstrap {

    private static final Logger log = LoggerFactory.getLogger(SchedulerBootstrap.class);

    private final RecoveryService recoveryService;
    private final NodeService nodeService;
    private final SchedulerService schedulerService;
    private final SchedulerProperties props;

    public SchedulerBootstrap(RecoveryService recoveryService, NodeService nodeService,
                              SchedulerService schedulerService, SchedulerProperties props) {
        this.recoveryService = recoveryService;
        this.nodeService = nodeService;
        this.schedulerService = schedulerService;
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        int replayed = recoveryService.recover();
        if (replayed == 0 && props.isSeedDemoNodes()) {
            seedDemoNodes();
        }
        int started = schedulerService.tick();
        log.info("调度器启动完成: 重放事件 {} 条, 首轮启动任务 {} 个, 事件日志={}",
                replayed, started, props.getEventLogPath());
    }

    private void seedDemoNodes() {
        nodeService.registerNode("node-a", 8, 16384, 2);
        nodeService.registerNode("node-b", 8, 16384, 2);
        nodeService.registerNode("node-c", 4, 8192, 0);
        log.info("已播种 3 个演示节点: node-a(8核/16GB/2GPU), node-b(8核/16GB/2GPU), node-c(4核/8GB/0GPU)");
    }

    @Scheduled(fixedDelayString = "${scheduler.tick-interval-millis:1000}")
    public void scheduledTick() {
        try {
            schedulerService.tick();
        } catch (Exception ex) {
            // 单轮失败不拖垮后台循环，也不污染已有状态（状态变更均在锁内原子完成）
            log.error("后台调度轮次异常，已跳过本轮", ex);
        }
    }
}

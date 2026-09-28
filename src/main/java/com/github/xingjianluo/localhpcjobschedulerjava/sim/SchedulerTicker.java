package com.github.xingjianluo.localhpcjobschedulerjava.sim;

import com.github.xingjianluo.localhpcjobschedulerjava.config.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 本地后台巡检：周期性做“超时检测 → 失败转移 → 整组调度”。
 *
 * <p>这只是把引擎能力按时间推进的驱动器；所有一致性逻辑仍在引擎锁内。
 * 单元测试通过 scheduler.tick-enabled=false 关掉它，改为手动触发以获得确定性。</p>
 */
@Component
public class SchedulerTicker {

    private static final Logger log = LoggerFactory.getLogger(SchedulerTicker.class);

    private final SchedulerEngine engine;
    private final SchedulerProperties props;

    public SchedulerTicker(SchedulerEngine engine, SchedulerProperties props) {
        this.engine = engine;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${scheduler.tick-interval-ms:2000}")
    public void tick() {
        if (!props.tickEnabled()) {
            return;
        }
        try {
            int affected = engine.detectTimeouts();
            int dispatched = engine.schedule();
            if (affected > 0 || dispatched > 0) {
                log.debug("tick：失败转移作业 {} 个，派发作业 {} 个", affected, dispatched);
            }
        } catch (Exception e) {
            // 巡检异常不能杀死调度线程
            log.error("tick 处理异常（已忽略，等待下一周期）", e);
        }
    }
}

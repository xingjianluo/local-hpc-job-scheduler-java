package com.github.xingjianluo.localhpcjobschedulerjava.bootstrap;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerState;
import com.github.xingjianluo.localhpcjobschedulerjava.core.TimeService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.JobService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.NodeService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.RecoveryService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerService;
import com.github.xingjianluo.localhpcjobschedulerjava.store.EventLog;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 调度器 Bean 装配。
 */
@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfig {

    @Bean
    public TimeService timeService() {
        return new TimeService();
    }

    @Bean
    public EventLog eventLog(SchedulerProperties props) {
        return new EventLog(props.getEventLogPath());
    }

    @Bean
    public SchedulerState schedulerState(EventLog eventLog) {
        return new SchedulerState(eventLog);
    }

    @Bean
    public NodeService nodeService(SchedulerState state, TimeService time, SchedulerProperties props) {
        return new NodeService(state, time, props);
    }

    @Bean
    public JobService jobService(SchedulerState state, TimeService time, SchedulerProperties props) {
        return new JobService(state, time, props);
    }

    @Bean
    public SchedulerService schedulerService(SchedulerState state, TimeService time,
                                             SchedulerProperties props) {
        return new SchedulerService(state, time, props);
    }

    @Bean
    public RecoveryService recoveryService(SchedulerState state, TimeService time,
                                           SchedulerProperties props) {
        return new RecoveryService(state, time, props);
    }
}

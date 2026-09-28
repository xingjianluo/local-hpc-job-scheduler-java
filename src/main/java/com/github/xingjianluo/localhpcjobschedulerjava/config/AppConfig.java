package com.github.xingjianluo.localhpcjobschedulerjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.localhpcjobschedulerjava.store.SnapshotStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 本地调度器的 Bean 装配。不接入任何真实集群或外部服务。
 */
@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class AppConfig {

    /**
     * snapshot-file 为空时返回 null，引擎即以纯内存模式运行（每次调用内部均有 null 判断）。
     */
    @Bean
    public SnapshotStore snapshotStore(SchedulerProperties properties,
                                       ObjectMapper objectMapper) {
        String location = properties.snapshotFile();
        if (location == null || location.isBlank()) {
            return null;
        }
        return new SnapshotStore(Path.of(location), objectMapper);
    }
}

package com.github.xingjianluo.localhpcjobschedulerjava.sim;

import com.github.xingjianluo.localhpcjobschedulerjava.config.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import com.github.xingjianluo.localhpcjobschedulerjava.service.NodeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * 可选的本地演示集群：仅当 scheduler.demo-cluster=true 且当前没有任何节点时，
 * 注册 3 个模拟节点，便于启动后直接用 HTTP 提交作业复现。
 * 默认关闭，不影响单元测试与干净启动。
 */
@Component
public class DemoClusterInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoClusterInitializer.class);

    private final NodeService nodeService;
    private final SchedulerProperties props;

    public DemoClusterInitializer(NodeService nodeService, SchedulerProperties props) {
        this.nodeService = nodeService;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!props.demoCluster()) {
            return;
        }
        if (!nodeService.list().isEmpty()) {
            log.info("快照中已存在节点，跳过演示集群初始化");
            return;
        }
        nodeService.register("node-a", new Resources(8, 8192, 2));
        nodeService.register("node-b", new Resources(8, 8192, 2));
        nodeService.register("node-c", new Resources(4, 4096, 1));
        log.info("已注册演示集群：node-a(8核/8192MB/2卡)、node-b(同)、node-c(4核/4096MB/1卡)");
    }
}

package com.github.xingjianluo.localhpcjobschedulerjava.service;

import com.github.xingjianluo.localhpcjobschedulerjava.model.ClusterNode;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 节点管理门面。自身无状态，所有一致性保证在 {@link SchedulerEngine} 内完成。
 */
@Service
public class NodeService {

    private final SchedulerEngine engine;

    public NodeService(SchedulerEngine engine) {
        this.engine = engine;
    }

    public ClusterNode register(String nodeId, Resources capacity) {
        return engine.registerNode(nodeId, capacity);
    }

    public void heartbeat(String nodeId) {
        engine.nodeHeartbeat(nodeId);
    }

    /** lost=true 模拟心跳失联；lost=false 模拟管理员主动下线。 */
    public ClusterNode markDown(String nodeId, boolean lost) {
        return engine.markNodeDown(nodeId, lost);
    }

    public ClusterNode recover(String nodeId) {
        return engine.recoverNode(nodeId);
    }

    public ClusterNode get(String nodeId) {
        return engine.getNode(nodeId);
    }

    public List<ClusterNode> list() {
        return engine.nodes();
    }
}

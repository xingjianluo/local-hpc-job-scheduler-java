package com.github.xingjianluo.localhpcjobschedulerjava.service;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerException;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerState;
import com.github.xingjianluo.localhpcjobschedulerjava.core.TimeService;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ComputeNode;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobState;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.NodeState;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.RejectReason;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ResourceRequest;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.WorkerAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 节点管理：注册、查询、标记掉线。
 * 掉线时其上运行中的作业尝试被判定为 LOST，并按失败策略（重试/终止）收敛；
 * 重试产生新 attempt，旧 attempt 的资源在同一临界区内精确释放，绝不重复计资源。
 */
public class NodeService {

    private static final Logger log = LoggerFactory.getLogger(NodeService.class);

    private final SchedulerState state;
    private final TimeService time;
    private final SchedulerProperties props;

    public NodeService(SchedulerState state, TimeService time, SchedulerProperties props) {
        this.state = state;
        this.time = time;
        this.props = props;
    }

    /** 注册节点（幂等：同 id 已存在则直接返回）。容量必须为正。 */
    public ComputeNode registerNode(String id, int cores, int memoryMb, int gpus) {
        if (id == null || id.isBlank()) {
            throw new SchedulerException(RejectReason.INVALID_CORES, "节点 id 不能为空");
        }
        if (cores <= 0 || memoryMb <= 0 || gpus < 0) {
            throw new SchedulerException(RejectReason.INVALID_CORES,
                    "节点容量非法: cores=" + cores + ", memoryMb=" + memoryMb + ", gpus=" + gpus);
        }
        synchronized (state.lock()) {
            ComputeNode existing = state.nodes().get(id);
            if (existing != null) {
                return existing;
            }
            ComputeNode node = new ComputeNode(id, cores, memoryMb, gpus);
            state.nodes().put(id, node);
            recomputeClusterCapacityLocked();
            state.eventLog().append("NODE_REGISTERED", time.nowMillis(), null, id, null,
                    List.of(String.valueOf(cores), String.valueOf(memoryMb), String.valueOf(gpus)));
            log.info("节点注册: id={}, 容量={}核/{}MB/{}卡", id, cores, memoryMb, gpus);
            return node;
        }
    }

    public ComputeNode getNode(String id) {
        synchronized (state.lock()) {
            ComputeNode node = state.nodes().get(id);
            if (node == null) {
                throw new SchedulerException(RejectReason.JOB_NOT_FOUND, "节点不存在: " + id);
            }
            return node;
        }
    }

    public List<ComputeNode> listNodes() {
        synchronized (state.lock()) {
            return new ArrayList<>(state.nodes().values());
        }
    }

    /**
     * 标记节点失效。幂等：重复标记返回 0。
     * 临界区内一次性：置 LOST、释放其上所有 attempt 资源、按重试/失败策略收敛相关作业。
     *
     * @return 被判定丢失的运行中尝试数量
     */
    public int markNodeLost(String nodeId, String reason) {
        synchronized (state.lock()) {
            ComputeNode node = state.nodes().get(nodeId);
            if (node == null) {
                throw new SchedulerException(RejectReason.JOB_NOT_FOUND, "节点不存在: " + nodeId);
            }
            if (node.state() == NodeState.LOST) {
                log.info("节点 {} 已处于 LOST，重复掉线通知幂等忽略", nodeId);
                return 0;
            }
            node.state(NodeState.LOST);
            state.eventLog().append("NODE_LOST", time.nowMillis(), null, nodeId, null,
                    List.of(reason == null ? "" : reason));
            log.info("节点掉线: id={}, reason={}, 其上运行中尝试={}", nodeId, reason,
                    node.runningAttempts().size());

            int lost = 0;
            // 复制一份，收敛过程会修改底层 map
            for (Map.Entry<String, String> e : new ArrayList<>(node.runningAttempts().entrySet())) {
                String attemptId = e.getKey();
                String jobId = e.getValue();
                Job job = state.jobs().get(jobId);
                if (job == null) {
                    continue;
                }
                lost++;
                FailurePolicy.convergeLostAttempt(state, time, props, job, attemptId, nodeId,
                        "节点掉线: " + reason);
            }
            // 掉线节点资源用量应已全部归零
            log.info("节点 {} 收敛完成，判定丢失尝试 {} 个，剩余记账用量={}核/{}MB/{}卡",
                    nodeId, lost, node.usedCores(), node.usedMemoryMb(), node.usedGpus());
            return lost;
        }
    }

    /** 重算集群总容量（遍历在线节点）。调用方持锁。 */
    void recomputeClusterCapacityLocked() {
        int cores = 0;
        int mem = 0;
        int gpus = 0;
        for (ComputeNode node : state.nodes().values()) {
            cores += node.totalCores();
            mem += node.totalMemoryMb();
            gpus += node.totalGpus();
        }
        state.clusterCapacity(new ResourceRequest(cores, mem, gpus, Integer.MAX_VALUE));
    }
}

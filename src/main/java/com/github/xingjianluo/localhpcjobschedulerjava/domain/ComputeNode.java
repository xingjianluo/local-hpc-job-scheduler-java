package com.github.xingjianluo.localhpcjobschedulerjava.domain;

import java.util.HashMap;
import java.util.Map;

/**
 * 本地模拟的计算节点及其已分配资源。
 * 节点级容量分配仅在持有调度器全局锁时修改。
 */
public class ComputeNode {

    private final String id;
    private final int totalCores;
    private final int totalMemoryMb;
    private final int totalGpus;
    private volatile NodeState state = NodeState.ONLINE;
    private int usedCores;
    private int usedMemoryMb;
    private int usedGpus;
    /** 正在该节点上运行的 attemptId -> 作业 id。 */
    private final Map<String, String> runningAttempts = new HashMap<>();

    public ComputeNode(String id, int totalCores, int totalMemoryMb, int totalGpus) {
        this.id = id;
        this.totalCores = totalCores;
        this.totalMemoryMb = totalMemoryMb;
        this.totalGpus = totalGpus;
    }

    public String id() {
        return id;
    }

    public int totalCores() {
        return totalCores;
    }

    public int totalMemoryMb() {
        return totalMemoryMb;
    }

    public int totalGpus() {
        return totalGpus;
    }

    public NodeState state() {
        return state;
    }

    public void state(NodeState state) {
        this.state = state;
    }

    public int usedCores() {
        return usedCores;
    }

    public int usedMemoryMb() {
        return usedMemoryMb;
    }

    public int usedGpus() {
        return usedGpus;
    }

    public Map<String, String> runningAttempts() {
        return runningAttempts;
    }

    // ===== 资源记账：调用方必须持有调度器全局锁 =====

    public int availableCores() {
        return totalCores - usedCores;
    }

    public int availableMemoryMb() {
        return totalMemoryMb - usedMemoryMb;
    }

    public int availableGpus() {
        return totalGpus - usedGpus;
    }

    /** 仅在线且各维剩余容量足够时可分配。 */
    public boolean canFit(ResourceRequest req) {
        return state == NodeState.ONLINE
                && req.cores() <= availableCores()
                && req.memoryMb() <= availableMemoryMb()
                && req.gpus() <= availableGpus();
    }

    /** 分配资源；重复 attemptId 视为幂等，不重复记账。超额分配直接拒绝。 */
    public void allocate(String attemptId, String jobId, ResourceRequest req) {
        if (runningAttempts.containsKey(attemptId)) {
            return;
        }
        if (!canFit(req)) {
            throw new IllegalStateException(
                    "node " + id + " 无法容纳请求 " + req + "，剩余=[" + availableCores()
                            + "核," + availableMemoryMb() + "MB," + availableGpus() + "卡]");
        }
        usedCores += req.cores();
        usedMemoryMb += req.memoryMb();
        usedGpus += req.gpus();
        runningAttempts.put(attemptId, jobId);
    }

    /** 释放资源；重复释放（attempt 不存在）直接忽略，保证不重复释放。 */
    public void release(String attemptId, ResourceRequest req) {
        if (!runningAttempts.containsKey(attemptId)) {
            return;
        }
        runningAttempts.remove(attemptId);
        usedCores -= req.cores();
        usedMemoryMb -= req.memoryMb();
        usedGpus -= req.gpus();
        if (usedCores < 0 || usedMemoryMb < 0 || usedGpus < 0) {
            throw new IllegalStateException("node " + id + " 资源用量在释放后为负，记账被破坏");
        }
    }
}

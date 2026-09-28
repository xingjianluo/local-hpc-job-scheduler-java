package com.github.xingjianluo.localhpcjobschedulerjava.core;

import com.github.xingjianluo.localhpcjobschedulerjava.domain.ComputeNode;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.JobArray;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ResourceRequest;
import com.github.xingjianluo.localhpcjobschedulerjava.store.EventLog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 调度器全部内存状态。
 * 所有变更都必须持有 {@link #lock()} 全局锁，保证并发下确定且一致。
 */
public class SchedulerState {

    private final Object lock = new Object();
    private final EventLog eventLog;
    private final Map<String, ComputeNode> nodes = new LinkedHashMap<>();
    private final Map<String, Job> jobs = new LinkedHashMap<>();
    private final Map<String, JobArray> arrays = new LinkedHashMap<>();
    /** 去重：作业提交键（客户端给定 id）。 */
    private final Map<String, String> dedupJobKeys = new LinkedHashMap<>();
    /** 去重：阵列提交键 -> 阵列 id。 */
    private final Map<String, String> dedupArrayKeys = new LinkedHashMap<>();
    /** attemptId -> jobId（包含已落账、等待完成/收敛的尝试）。 */
    private final Map<String, String> attemptIndex = new LinkedHashMap<>();
    /** 集群总容量（用于快速判定超出容量）。 */
    private ResourceRequest clusterCapacity = new ResourceRequest(0, 0, 0, Integer.MAX_VALUE);
    /** 提交序号计数器（含阵列任务）。 */
    private long submitSequence = 0;

    public SchedulerState(EventLog eventLog) {
        this.eventLog = eventLog;
    }

    public Object lock() {
        return lock;
    }

    public EventLog eventLog() {
        return eventLog;
    }

    public Map<String, ComputeNode> nodes() {
        return nodes;
    }

    public Map<String, Job> jobs() {
        return jobs;
    }

    public Map<String, JobArray> arrays() {
        return arrays;
    }

    public Map<String, String> dedupJobKeys() {
        return dedupJobKeys;
    }

    public Map<String, String> dedupArrayKeys() {
        return dedupArrayKeys;
    }

    public Map<String, String> attemptIndex() {
        return attemptIndex;
    }

    public ResourceRequest clusterCapacity() {
        return clusterCapacity;
    }

    public void clusterCapacity(ResourceRequest capacity) {
        this.clusterCapacity = capacity;
    }

    /** 生成下一个全局提交序号。 */
    public long nextSubmitSeq() {
        return ++submitSequence;
    }

    /** 重放后对齐序号到已知最大值，保证重启后序号继续单调。 */
    public void advanceSubmitSeq(long seq) {
        if (seq > submitSequence) {
            submitSequence = seq;
        }
    }

    /** 当前所有 PENDING 作业（确定性顺序由调度器负责）。 */
    public List<Job> pendingJobs() {
        List<Job> result = new ArrayList<>();
        for (Job job : jobs.values()) {
            if (!job.isTerminal() && job.state() == com.github.xingjianluo.localhpcjobschedulerjava.domain.JobState.PENDING) {
                result.add(job);
            }
        }
        return result;
    }
}

package com.github.xingjianluo.localhpcjobschedulerjava.service;

import com.github.xingjianluo.localhpcjobschedulerjava.model.ClusterNode;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Task;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.JobView;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.NodeView;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.TaskView;

import java.util.List;

/** 领域模型到只读视图的转换，集中在一处便于解释字段含义。 */
public final class ViewMapper {

    private ViewMapper() {
    }

    public static JobView jobView(Job job) {
        List<TaskView> tasks = job.tasks().stream()
                .map(ViewMapper::taskView)
                .toList();
        return new JobView(
                job.id(),
                job.name(),
                job.arraySize(),
                job.perTaskRequest().cpus(),
                job.perTaskRequest().memoryMb(),
                job.perTaskRequest().gpus(),
                job.durationLimitMs(),
                job.priority(),
                job.state(),
                job.queueReason(),
                job.reasonDetail(),
                job.retryEligibleAtMs(),
                tasks);
    }

    public static TaskView taskView(Task t) {
        return new TaskView(
                t.id(),
                t.index(),
                t.state(),
                t.attemptNo(),
                t.nodeId(),
                t.startedAtMs(),
                t.lastHeartbeatMs(),
                t.failReason());
    }

    public static NodeView nodeView(ClusterNode n) {
        return new NodeView(
                n.nodeId(),
                n.state(),
                n.capacity().cpus(),
                n.capacity().memoryMb(),
                n.capacity().gpus(),
                n.used().cpus(),
                n.used().memoryMb(),
                n.used().gpus(),
                n.lastHeartbeatMs());
    }
}

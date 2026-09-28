package com.github.xingjianluo.localhpcjobschedulerjava.web;

import com.github.xingjianluo.localhpcjobschedulerjava.model.ClusterNode;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import com.github.xingjianluo.localhpcjobschedulerjava.service.JobService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.NodeService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.ViewMapper;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.ActionResult;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.JobView;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.NodeView;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.SnapshotView;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.SubmitJobRequest;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.SubmitResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 本地集群作业调度的全部 HTTP 入口。纯本地模拟，无外部依赖。
 */
@RestController
@RequestMapping("/api")
public class SchedulerController {

    private final NodeService nodeService;
    private final JobService jobService;

    public SchedulerController(NodeService nodeService, JobService jobService) {
        this.nodeService = nodeService;
        this.jobService = jobService;
    }

    // ---------------- 节点 ----------------

    public record RegisterNodeRequest(String nodeId, Long cpus, Long memoryMb, Long gpus) {
    }

    @PostMapping("/nodes")
    public NodeView registerNode(@RequestBody RegisterNodeRequest req) {
        Resources cap = new Resources(
                req.cpus() == null ? 0 : req.cpus(),
                req.memoryMb() == null ? 0 : req.memoryMb(),
                req.gpus() == null ? 0 : req.gpus());
        return ViewMapper.nodeView(nodeService.register(req.nodeId(), cap));
    }

    @PostMapping("/nodes/{nodeId}/heartbeat")
    public ActionResult nodeHeartbeat(@PathVariable String nodeId) {
        nodeService.heartbeat(nodeId);
        return ActionResult.of("node-heartbeat", "节点心跳已记录: " + nodeId);
    }

    /** lost=true 心跳失联；lost=false 主动下线。 */
    @PostMapping("/nodes/{nodeId}/down")
    public NodeView nodeDown(@PathVariable String nodeId,
                             @RequestParam(defaultValue = "true") boolean lost) {
        return ViewMapper.nodeView(nodeService.markDown(nodeId, lost));
    }

    @PostMapping("/nodes/{nodeId}/recover")
    public NodeView nodeRecover(@PathVariable String nodeId) {
        return ViewMapper.nodeView(nodeService.recover(nodeId));
    }

    @GetMapping("/nodes")
    public List<NodeView> nodes() {
        return nodeService.list().stream().map(ViewMapper::nodeView).toList();
    }

    // ---------------- 作业 ----------------

    @PostMapping("/jobs")
    public org.springframework.http.ResponseEntity<SubmitResult> submit(
            @RequestBody SubmitJobRequest request) {
        SubmitResult result = jobService.submit(request);
        // accepted=false（容量/业务预检拒绝）用 422 表达，与接受/幂等重复(200)区分
        if (!result.accepted()) {
            return org.springframework.http.ResponseEntity.status(
                    org.springframework.http.HttpStatus.UNPROCESSABLE_ENTITY).body(result);
        }
        return org.springframework.http.ResponseEntity.ok(result);
    }

    @GetMapping("/jobs")
    public List<JobView> jobs() {
        return jobService.list().stream().map(ViewMapper::jobView).toList();
    }

    @GetMapping("/jobs/{jobId}")
    public JobView job(@PathVariable String jobId) {
        return ViewMapper.jobView(jobService.get(jobId));
    }

    @PostMapping("/jobs/{jobId}/cancel")
    public JobView cancel(@PathVariable String jobId) {
        return ViewMapper.jobView(jobService.cancel(jobId));
    }

    // ---------------- 工作进程上报 ----------------

    @PostMapping("/tasks/{taskId}/heartbeat")
    public ActionResult heartbeat(@PathVariable String taskId) {
        jobService.heartbeat(taskId);
        return ActionResult.of("task-heartbeat", "心跳已记录: " + taskId);
    }

    @PostMapping("/tasks/{taskId}/complete")
    public JobView complete(@PathVariable String taskId) {
        return ViewMapper.jobView(jobService.complete(taskId));
    }

    // ---------------- 调度与巡检（测试可手动触发，确定性验证） ----------------

    @PostMapping("/schedule")
    public Map<String, Object> schedule() {
        int n = jobService.runSchedule();
        return Map.of("dispatchedJobs", n);
    }

    @PostMapping("/detect-timeouts")
    public Map<String, Object> detectTimeouts() {
        int n = jobService.runTimeoutDetection();
        return Map.of("affectedJobs", n);
    }

    @GetMapping("/snapshot")
    public SnapshotView snapshot() {
        long now = System.currentTimeMillis();
        List<NodeView> nv = nodeService.list().stream().map(ViewMapper::nodeView).toList();
        List<JobView> jv = jobService.list().stream().map(ViewMapper::jobView).toList();
        return new SnapshotView(now, nv, jv);
    }
}

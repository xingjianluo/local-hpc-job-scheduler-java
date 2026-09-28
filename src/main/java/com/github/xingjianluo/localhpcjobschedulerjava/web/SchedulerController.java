package com.github.xingjianluo.localhpcjobschedulerjava.web;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerException;
import com.github.xingjianluo.localhpcjobschedulerjava.core.TimeService;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ComputeNode;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.RejectReason;
import com.github.xingjianluo.localhpcjobschedulerjava.domain.ResourceRequest;
import com.github.xingjianluo.localhpcjobschedulerjava.service.JobService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.NodeService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerService;
import com.github.xingjianluo.localhpcjobschedulerjava.web.dto.Dtos;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * 本地验证用 REST 接口（不接真实集群/外部服务）。
 */
@RestController
@RequestMapping("/api")
public class SchedulerController {

    private final NodeService nodeService;
    private final JobService jobService;
    private final SchedulerService schedulerService;
    private final TimeService time;

    public SchedulerController(NodeService nodeService, JobService jobService,
                               SchedulerService schedulerService, TimeService time) {
        this.nodeService = nodeService;
        this.jobService = jobService;
        this.schedulerService = schedulerService;
        this.time = time;
    }

    @PostMapping("/nodes")
    public Dtos.NodeView registerNode(@RequestBody Dtos.NodeRequest req) {
        if (req == null || req.nodeId() == null || req.cores() == null
                || req.memoryMb() == null || req.gpus() == null) {
            throw new SchedulerException(RejectReason.INVALID_CORES,
                    "nodeId/cores/memoryMb/gpus 均必填（gpus 可为 0）");
        }
        return toView(nodeService.registerNode(req.nodeId(), req.cores(),
                req.memoryMb(), req.gpus()));
    }

    @GetMapping("/nodes")
    public List<Dtos.NodeView> listNodes() {
        return nodeService.listNodes().stream().map(SchedulerController::toView).toList();
    }

    @PostMapping("/node-lost/{nodeId}")
    public Dtos.TickResponse markNodeLost(@PathVariable String nodeId,
                                          @RequestParam(defaultValue = "simulated-loss") String reason) {
        int lost = nodeService.markNodeLost(nodeId, reason);
        int started = schedulerService.tick();
        return new Dtos.TickResponse(lost + started, time.nowMillis());
    }

    @PostMapping("/jobs")
    public Dtos.SubmitResponse submitJob(@RequestBody Dtos.SubmitJobRequest req) {
        if (req == null) {
            throw new SchedulerException(RejectReason.INVALID_CORES, "请求体为空");
        }
        Job job = jobService.submitJob(req.clientId(), req.clientId(),
                new ResourceRequest(nz(req.cores()), nz(req.memoryMb()),
                        gz(req.gpus()), nz(req.durationLimitSec())),
                req.priority(), req.maxRetries());
        return new Dtos.SubmitResponse(job.id(), null, List.of(job.id()), job.state().name());
    }

    @PostMapping("/arrays")
    public Dtos.SubmitResponse submitArray(@RequestBody Dtos.SubmitArrayRequest req) {
        if (req == null || req.taskCount() == null) {
            throw new SchedulerException(RejectReason.ZERO_TASKS, "taskCount 必填且为正");
        }
        List<Job> tasks = jobService.submitArray(req.clientId(), req.clientId(), req.taskCount(),
                new ResourceRequest(nz(req.cores()), nz(req.memoryMb()),
                        gz(req.gpus()), nz(req.durationLimitSec())),
                req.priority(), req.maxRetries());
        List<String> ids = new ArrayList<>();
        for (Job t : tasks) {
            ids.add(t.id());
        }
        String arrayId = tasks.isEmpty() ? null : tasks.get(0).arrayId();
        return new Dtos.SubmitResponse(null, arrayId, ids, "PENDING");
    }

    @GetMapping("/jobs/{jobId}")
    public Dtos.JobView getJob(@PathVariable String jobId) {
        return toJobView(jobService.getJob(jobId));
    }

    @PostMapping("/jobs/{jobId}/cancel")
    public Dtos.JobView cancel(@PathVariable String jobId,
                               @RequestBody(required = false) Dtos.CancelRequest req) {
        String reason = req == null ? "api-cancel" : req.reason();
        return toJobView(jobService.cancel(jobId, reason));
    }

    @PostMapping("/jobs/{jobId}/heartbeat")
    public Dtos.JobView heartbeat(@PathVariable String jobId,
                                  @RequestBody Dtos.HeartbeatRequest req) {
        if (req == null || req.attemptId() == null) {
            throw new SchedulerException(RejectReason.ILLEGAL_STATE, "attemptId 必填");
        }
        jobService.heartbeat(jobId, req.attemptId());
        return toJobView(jobService.getJob(jobId));
    }

    @PostMapping("/jobs/{jobId}/complete")
    public Dtos.JobView complete(@PathVariable String jobId,
                                 @RequestBody Dtos.CompleteRequest req) {
        if (req == null || req.attemptId() == null) {
            throw new SchedulerException(RejectReason.ILLEGAL_STATE, "attemptId 必填");
        }
        return toJobView(jobService.complete(jobId, req.attemptId()));
    }

    @PostMapping("/scheduler/tick")
    public Dtos.TickResponse tick() {
        int started = schedulerService.tick();
        return new Dtos.TickResponse(started, time.nowMillis());
    }

    private static int nz(Integer v) {
        return v == null ? -1 : v; // 非法值交由校验层归类拒绝
    }

    private static int gz(Integer v) {
        return v == null ? 0 : v; // GPU 缺省视为 0
    }

    private static Dtos.NodeView toView(ComputeNode n) {
        return new Dtos.NodeView(n.id(), n.state().name(), n.totalCores(), n.usedCores(),
                n.totalMemoryMb(), n.usedMemoryMb(), n.totalGpus(), n.usedGpus(),
                n.runningAttempts().size());
    }

    private static Dtos.JobView toJobView(Job j) {
        String nodeId = j.currentAttempt() == null ? null : j.currentAttempt().nodeId();
        String attemptId = j.currentAttempt() == null ? null : j.currentAttempt().attemptId();
        return new Dtos.JobView(j.id(), j.arrayId(), j.arrayIndex(), j.state().name(),
                j.priority(), j.request().cores(), j.request().memoryMb(), j.request().gpus(),
                j.request().durationLimitSec(), j.attemptCount(), j.maxRetries(),
                nodeId, attemptId, j.queueReason(),
                j.terminationCause() == null ? null : j.terminationCause().name(),
                j.detail());
    }
}

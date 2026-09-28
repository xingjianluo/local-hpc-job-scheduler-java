package com.github.xingjianluo.localhpcjobschedulerjava.service;

import com.github.xingjianluo.localhpcjobschedulerjava.exception.InvalidRequestException;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.model.Resources;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.SubmitJobRequest;
import com.github.xingjianluo.localhpcjobschedulerjava.web.Dtos.SubmitResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 作业门面：负责 HTTP 层请求的格式校验，把业务判定交给 {@link SchedulerEngine}，
 * 再把引擎的接受/重复/拒绝结果翻译成 {@link SubmitResult}。
 */
@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final SchedulerEngine engine;

    public JobService(SchedulerEngine engine) {
        this.engine = engine;
    }

    public SubmitResult submit(SubmitJobRequest request) {
        if (request == null) {
            throw new InvalidRequestException("请求体为空");
        }
        int arraySize = request.arraySize() == null ? 1 : request.arraySize();
        long cpus = valueOrZero(request.cpus());
        long mem = valueOrZero(request.memoryMb());
        long gpus = valueOrZero(request.gpus());
        long duration = valueOrZero(request.durationLimitMs());

        log.info("收到提交请求 jobId={} name='{}' arraySize={} cpus={} memMb={} gpus={} "
                        + "durationLimitMs={} priority={} maxRetries={} backoffMs={}",
                request.jobId(), request.name(), arraySize, cpus, mem, gpus,
                duration, request.priority(), request.maxRetries(), request.retryBackoffMs());

        Resources perTask = new Resources(cpus, mem, gpus);
        SchedulerEngine.SubmitOutcome outcome = engine.submitJob(
                request.jobId(), request.name(), arraySize, perTask, duration,
                request.priority(), request.maxRetries(), request.retryBackoffMs());

        return switch (outcome.kind()) {
            case ACCEPTED -> SubmitResult.accepted(ViewMapper.jobView(outcome.job()));
            case DUPLICATE -> SubmitResult.duplicate(ViewMapper.jobView(outcome.job()));
            case REJECTED -> SubmitResult.rejected(request.jobId(),
                    outcome.rejectCode(), outcome.message());
        };
    }

    private static long valueOrZero(Long v) {
        return v == null ? 0L : v;
    }

    public Job get(String jobId) {
        return engine.getJob(jobId);
    }

    public List<Job> list() {
        return engine.jobs();
    }

    public Job cancel(String jobId) {
        return engine.cancelJob(jobId);
    }

    public void heartbeat(String taskId) {
        engine.taskHeartbeat(taskId);
    }

    public Job complete(String taskId) {
        return engine.taskComplete(taskId);
    }

    public int runSchedule() {
        return engine.schedule();
    }

    public int runTimeoutDetection() {
        return engine.detectTimeouts();
    }
}

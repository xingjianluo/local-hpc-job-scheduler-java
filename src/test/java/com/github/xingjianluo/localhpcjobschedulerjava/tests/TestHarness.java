package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerProperties;
import com.github.xingjianluo.localhpcjobschedulerjava.core.SchedulerState;
import com.github.xingjianluo.localhpcjobschedulerjava.core.TimeService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.JobService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.NodeService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.RecoveryService;
import com.github.xingjianluo.localhpcjobschedulerjava.service.SchedulerService;
import com.github.xingjianluo.localhpcjobschedulerjava.store.EventLog;

import java.nio.file.Path;

/**
 * 测试夹具：手动可控时钟 + 临时事件日志，四个服务直连（不经 Spring）。
 */
public class TestHarness {

    /** 可手动推进的时钟。 */
    static class FixedClock extends TimeService {
        private long t;

        FixedClock(long start) {
            this.t = start;
        }

        @Override
        public long nowMillis() {
            return t;
        }

        void advance(long deltaMillis) {
            this.t += deltaMillis;
        }

        void set(long t) {
            this.t = t;
        }
    }

    public final FixedClock clock;
    public final SchedulerProperties props;
    public final EventLog eventLog;
    public final SchedulerState state;
    public final NodeService nodeService;
    public final JobService jobService;
    public final SchedulerService schedulerService;
    public final RecoveryService recoveryService;

    private TestHarness(FixedClock clock, SchedulerProperties props, EventLog eventLog,
                        SchedulerState state, NodeService nodeService, JobService jobService,
                        SchedulerService schedulerService, RecoveryService recoveryService) {
        this.clock = clock;
        this.props = props;
        this.eventLog = eventLog;
        this.state = state;
        this.nodeService = nodeService;
        this.jobService = jobService;
        this.schedulerService = schedulerService;
        this.recoveryService = recoveryService;
    }

    /** 构建一套全新夹具（空事件日志）。 */
    public static TestHarness create(Path eventLogPath) {
        return create(eventLogPath, new SchedulerProperties());
    }

    public static TestHarness create(Path eventLogPath, SchedulerProperties props) {
        FixedClock clock = new FixedClock(1_000_000L);
        EventLog eventLog = new EventLog(eventLogPath.toString());
        SchedulerState state = new SchedulerState(eventLog);
        NodeService nodeService = new NodeService(state, clock, props);
        JobService jobService = new JobService(state, clock, props);
        SchedulerService schedulerService = new SchedulerService(state, clock, props);
        RecoveryService recoveryService = new RecoveryService(state, clock, props);
        return new TestHarness(clock, props, eventLog, state, nodeService, jobService,
                schedulerService, recoveryService);
    }

    /** 基于同一份事件日志、全新的内存状态与时钟，模拟"进程重启恢复"。 */
    public TestHarness restart(long clockStart) {
        FixedClock clock2 = new FixedClock(clockStart);
        SchedulerState state2 = new SchedulerState(eventLog);
        NodeService ns = new NodeService(state2, clock2, props);
        JobService js = new JobService(state2, clock2, props);
        SchedulerService ss = new SchedulerService(state2, clock2, props);
        RecoveryService rs = new RecoveryService(state2, clock2, props);
        int replayed = rs.recover();
        return new TestHarness(clock2, props, eventLog, state2, ns, js, ss, rs) {
            @Override
            public String toString() {
                return "restarted(replayed=" + replayed + ")";
            }
        };
    }
}

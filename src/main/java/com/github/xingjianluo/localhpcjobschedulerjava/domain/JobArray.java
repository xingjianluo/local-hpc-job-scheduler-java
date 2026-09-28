package com.github.xingjianluo.localhpcjobschedulerjava.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * 作业阵列：一组同构任务，整组调度（gang scheduling），
 * 要么一次性全部满足资源，要么整体继续排队。
 */
public class JobArray {

    private final String id;
    private final List<Job> tasks = new ArrayList<>();
    private final long submittedAtMillis;

    public JobArray(String id, long submittedAtMillis) {
        this.id = id;
        this.submittedAtMillis = submittedAtMillis;
    }

    public String id() {
        return id;
    }

    public List<Job> tasks() {
        return tasks;
    }

    public long submittedAtMillis() {
        return submittedAtMillis;
    }

    public boolean allTerminal() {
        return tasks.stream().allMatch(Job::isTerminal);
    }
}

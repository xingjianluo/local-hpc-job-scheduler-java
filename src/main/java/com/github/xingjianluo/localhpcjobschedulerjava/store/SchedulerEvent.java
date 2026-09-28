package com.github.xingjianluo.localhpcjobschedulerjava.store;

import java.util.List;

/**
 * 一条本地持久化事件。事件只追加、不修改，进程重启后通过重放恢复一致状态。
 *
 * @param seq      单调递增序号
 * @param type     事件类型
 * @param millis   事件发生时间
 * @param jobId    作业 id（可空）
 * @param nodeId   节点 id（可空）
 * @param attemptId 尝试 id（可空）
 * @param payload  附加键值（如状态、成因、容量等）
 */
public record SchedulerEvent(long seq,
                             String type,
                             long millis,
                             String jobId,
                             String nodeId,
                             String attemptId,
                             List<String> payload) {
}

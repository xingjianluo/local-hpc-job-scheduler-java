package com.github.xingjianluo.localhpcjobschedulerjava.store;

import com.github.xingjianluo.localhpcjobschedulerjava.model.Job;
import com.github.xingjianluo.localhpcjobschedulerjava.model.ClusterNode;

import java.util.List;

/**
 * 落盘快照：节点表 + 作业表 + 提交序号水位。
 * 用不可变记录承载，序列化/反序列化均以整份快照为单位（本地 JSON 文件）。
 */
public record PersistentState(List<ClusterNode> nodes, List<Job> jobs, long submitSeqWatermark) {
}

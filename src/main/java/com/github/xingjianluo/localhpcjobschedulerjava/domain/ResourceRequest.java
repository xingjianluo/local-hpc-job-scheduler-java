package com.github.xingjianluo.localhpcjobschedulerjava.domain;

/**
 * 单个任务的资源请求。
 *
 * @param cores          计算核数（>0）
 * @param memoryMb       内存 MB（>0）
 * @param gpus           加速卡数量（>=0）
 * @param durationLimitSec 时长上限秒（>0）
 */
public record ResourceRequest(int cores, int memoryMb, int gpus, int durationLimitSec) {

    /** 是否为不超过 other 的资源量（逐维比较）。 */
    public boolean fitsIn(ResourceRequest other) {
        return this.cores <= other.cores
                && this.memoryMb <= other.memoryMb
                && this.gpus <= other.gpus;
    }
}

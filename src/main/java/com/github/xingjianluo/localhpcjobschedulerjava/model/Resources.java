package com.github.xingjianluo.localhpcjobschedulerjava.model;

/**
 * 资源向量：计算核数、内存(MB)、加速卡数量。
 * 不可变值对象，所有运算均返回新实例。
 *
 * @param cpus     计算核数
 * @param memoryMb 内存大小，单位 MB
 * @param gpus     加速卡数量
 */
public record Resources(long cpus, long memoryMb, long gpus) {

    public static final Resources ZERO = new Resources(0, 0, 0);

    /** 任一维度是否为负数（提交校验用）。 */
    public boolean hasNegativeDimension() {
        return cpus < 0 || memoryMb < 0 || gpus < 0;
    }

    /** 本资源向量是否能放入给定容量（三维均不超过）。 */
    public boolean fitsIn(Resources capacity) {
        return cpus <= capacity.cpus
                && memoryMb <= capacity.memoryMb
                && gpus <= capacity.gpus;
    }

    /** 向量逐维相加。 */
    public Resources plus(Resources other) {
        return new Resources(
                cpus + other.cpus,
                memoryMb + other.memoryMb,
                gpus + other.gpus);
    }

    /** 向量逐维相减（调用方需保证结果不为负，{@link #fitsIn(Resources)} 可先行判断）。 */
    public Resources minus(Resources other) {
        return new Resources(
                cpus - other.cpus,
                memoryMb - other.memoryMb,
                gpus - other.gpus);
    }

    /** 逐维放大 n 倍，用于单任务请求推算阵列总请求。 */
    public Resources times(long n) {
        if (n <= 0) {
            return ZERO;
        }
        return new Resources(cpus * n, memoryMb * n, gpus * n);
    }

    /** 与 other 逐维取较大值，用于汇总集群单节点最大可容纳请求。 */
    public Resources maxPerDimension(Resources other) {
        return new Resources(
                Math.max(cpus, other.cpus),
                Math.max(memoryMb, other.memoryMb),
                Math.max(gpus, other.gpus));
    }

    @Override
    public String toString() {
        return "(cpus=" + cpus + ", memMb=" + memoryMb + ", gpus=" + gpus + ")";
    }
}

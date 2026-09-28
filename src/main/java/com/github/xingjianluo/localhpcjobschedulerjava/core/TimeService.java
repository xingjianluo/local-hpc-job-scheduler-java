package com.github.xingjianluo.localhpcjobschedulerjava.core;

/**
 * 时间源：生产用系统时钟，测试可注入固定/步进时钟。
 */
public class TimeService {

    public long nowMillis() {
        return System.currentTimeMillis();
    }
}

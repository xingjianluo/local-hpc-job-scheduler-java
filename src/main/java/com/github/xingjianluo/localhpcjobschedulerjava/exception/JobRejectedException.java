package com.github.xingjianluo.localhpcjobschedulerjava.exception;

import com.github.xingjianluo.localhpcjobschedulerjava.model.RejectCode;

/**
 * 作业/阵列提交时未通过校验或容量预检，被明确拒绝。
 * 拒绝原因必须可区分，见 {@link RejectCode}。
 */
public class JobRejectedException extends RuntimeException {

    private final RejectCode code;

    public JobRejectedException(RejectCode code, String message) {
        super(message);
        this.code = code;
    }

    public RejectCode code() {
        return code;
    }
}

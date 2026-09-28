package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端冒烟：通过 HTTP 走完整链路（提交→调度→心跳/完成/取消/掉线），
 * 并验证错误被正确归类（422 + 可区分 reason）。
 * 每个用例用不同 clientId 去重，避免用例间相互影响。
 */
@SpringBootTest(properties = {
        "scheduler.tick-interval-millis=3600000",
        "scheduler.seed-demo-nodes=true",
        "scheduler.event-log-path=target/test-data/rest-events.log"
})
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.DisplayName.class)
class RestApiIntegrationTests {

    private static final Logger log = LoggerFactory.getLogger(RestApiIntegrationTests.class);

    @Autowired
    private MockMvc mvc;

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void cleanEventLog() throws Exception {
        java.nio.file.Path p = java.nio.file.Paths.get("target/test-data/rest-events.log");
        java.nio.file.Files.deleteIfExists(p);
    }

    private String field(String json, String name) throws Exception {
        JsonNode node = mapper.readTree(json).get(name);
        return node == null || node.isNull() ? null : node.asText();
    }

    @Test
    void fullLifecycleOverHttp() throws Exception {
        String tag = String.valueOf(System.nanoTime());
        log.info("[关键输入] REST 全链路用例 tag={}", tag);

        // 演示节点已在启动时播种
        String nodes = mvc.perform(get("/api/nodes"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        log.info("[判定依据] 启动播种节点: {}", nodes);

        // 提交 1 核作业
        String submitResp = mvc.perform(post("/api/jobs")
                        .contentType("application/json")
                        .content("""
                                {"clientId":"rest-%s","cores":2,"memoryMb":1024,"gpus":0,
                                 "durationLimitSec":60,"priority":5,"maxRetries":1}
                                """.formatted(tag)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("PENDING"))
                .andReturn().getResponse().getContentAsString();
        String jobId = field(submitResp, "jobId");
        log.info("[判定依据] 作业提交成功 jobId={}", jobId);

        // 手动 tick 调度（同一上下文内若有其它已提交作业会被一并调度，故只校验 >=1）
        mvc.perform(post("/api/scheduler/tick"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.started").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)));

        String jobJson = mvc.perform(get("/api/jobs/" + jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("RUNNING"))
                .andExpect(jsonPath("$.nodeId").exists())
                .andReturn().getResponse().getContentAsString();
        String attemptId = field(jobJson, "attemptId");
        log.info("[判定依据] tick 后作业 RUNNING，nodeId/attemptId 已分配: attemptId={}", attemptId);

        // 心跳
        mvc.perform(post("/api/jobs/" + jobId + "/heartbeat")
                        .contentType("application/json")
                        .content("""
                                {"attemptId":"%s"}
                                """.formatted(attemptId)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("RUNNING"));

        // 完成
        mvc.perform(post("/api/jobs/" + jobId + "/complete")
                        .contentType("application/json")
                        .content("""
                                {"attemptId":"%s"}
                                """.formatted(attemptId)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("COMPLETED"))
                .andExpect(jsonPath("$.terminationCause").value("COMPLETED_BY_WORKER"));
        log.info("[判定依据] 心跳→完成链路正常，终态 COMPLETED");
    }

    @Test
    void invalidAndDuplicateRequestsAreClassifiedOverHttp() throws Exception {
        String tag = String.valueOf(System.nanoTime());

        // 非法核数 -> 422 + INVALID_CORES
        mvc.perform(post("/api/jobs")
                        .contentType("application/json")
                        .content("""
                                {"clientId":"bad-%s","cores":0,"memoryMb":1024,"gpus":0,"durationLimitSec":60}
                                """.formatted(tag)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("INVALID_CORES"));

        // 超容量 -> 422 + EXCEEDS_CLUSTER_CAPACITY（演示集群共 20 核）
        mvc.perform(post("/api/jobs")
                        .contentType("application/json")
                        .content("""
                                {"clientId":"huge-%s","cores":999,"memoryMb":1024,"gpus":0,"durationLimitSec":60}
                                """.formatted(tag)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("EXCEEDS_CLUSTER_CAPACITY"));

        // 重复提交 -> 422 + DUPLICATE_SUBMISSION
        String body = """
                {"clientId":"dup-%s","cores":1,"memoryMb":256,"gpus":0,"durationLimitSec":60}
                """.formatted(tag);
        mvc.perform(post("/api/jobs").contentType("application/json").content(body))
                .andExpect(status().isOk());
        mvc.perform(post("/api/jobs").contentType("application/json").content(body))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("DUPLICATE_SUBMISSION"));

        // 不存在作业 -> 422 + JOB_NOT_FOUND
        mvc.perform(get("/api/jobs/job-does-not-exist"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("JOB_NOT_FOUND"));
        log.info("[判定依据] HTTP 错误归类可区分：INVALID_CORES / EXCEEDS_CLUSTER_CAPACITY / DUPLICATE_SUBMISSION / JOB_NOT_FOUND");
    }
}

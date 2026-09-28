package com.github.xingjianluo.localhpcjobschedulerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端 HTTP 链路：注册节点 → 提交/拒绝（可区分错误码）→ 重复提交幂等
 * → 完成/取消竞争 → 节点失联 → 重试，错误归类为 422/409/404。
 *
 * <p>使用随机节点/作业 ID，避免依赖测试顺序；后台 tick 在测试配置中关闭。</p>
 */
@SpringBootTest(properties = {
        "scheduler.tick-enabled=false",
        "scheduler.snapshot-file=target/test-data/web-snapshot.json",
        "scheduler.heartbeat-timeout-ms=999999",
        "scheduler.node-timeout-ms=999999"
})
@AutoConfigureMockMvc
@TestMethodOrder(OrderAnnotation.class)
class SchedulerWebTest {

    private static final Logger log = LoggerFactory.getLogger(SchedulerWebTest.class);
    private static final ObjectMapper M = new ObjectMapper();

    @Autowired
    private MockMvc mvc;

    private String json(Object body) throws Exception {
        return M.writeValueAsString(body);
    }

    @Test
    @Order(1)
    void fullLifecycleOverHttp() throws Exception {
        String n1 = "web-n1-" + UUID.randomUUID();
        String n2 = "web-n2-" + UUID.randomUUID();

        // 注册两个节点
        mvc.perform(post("/api/nodes").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("nodeId", n1, "cpus", 8, "memoryMb", 8192, "gpus", 2))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("UP"));
        mvc.perform(post("/api/nodes").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("nodeId", n2, "cpus", 8, "memoryMb", 8192, "gpus", 2))))
                .andExpect(status().isOk());
        log.info("[关键输入] HTTP 注册节点 {}、{}，各 8 核", n1, n2);

        // 提交阵列 2×4 核，直接 RUNNING
        String arr = "web-arr-" + UUID.randomUUID();
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("jobId", arr, "name", "阵列", "arraySize", 2,
                                "cpus", 4, "memoryMb", 1, "durationLimitMs", 60000))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.job.state").value("RUNNING"));

        // 重复提交：duplicate=true，不重复占用
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("jobId", arr, "name", "阵列", "arraySize", 2,
                                "cpus", 4, "memoryMb", 1, "durationLimitMs", 60000))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true));
        log.info("[判定依据] HTTP 重复提交 arr 返回 duplicate=true");

        // 非法时长：422 + INVALID_DURATION_LIMIT
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("jobId", "bad", "name", "坏作业",
                                "cpus", 1, "durationLimitMs", 0))))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.rejectCode").value("INVALID_DURATION_LIMIT"));

        // 超容量：422 + EXCEEDS_MAX_NODE（16 核任务任何节点放不下）
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("jobId", "huge", "name", "超大",
                                "cpus", 16, "durationLimitMs", 1000))))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.rejectCode").value("EXCEEDS_MAX_NODE"));
        log.info("[判定依据] 非法时长与超容量分别返回 422 且 rejectCode 可区分");

        // 第一个任务重复完成：两次都 200，作业先 PARTIAL 再 COMPLETED
        mvc.perform(post("/api/tasks/" + arr + ".0/complete")).andExpect(status().isOk());
        mvc.perform(post("/api/tasks/" + arr + ".0/complete"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value(arr));
        mvc.perform(post("/api/tasks/" + arr + ".1/complete"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("COMPLETED"));
        log.info("[判定依据] 阵列任务重复完成幂等，最终 state=COMPLETED");

        // 对已完成作业取消：409
        mvc.perform(post("/api/jobs/" + arr + "/cancel"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("STATE_CONFLICT"));
        // 不存在：404
        mvc.perform(get("/api/jobs/missing-" + UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    @Test
    @Order(2)
    void nodeLossAndManualTimeoutDetectionOverHttp() throws Exception {
        String n1 = "web-lost-" + UUID.randomUUID();
        mvc.perform(post("/api/nodes").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("nodeId", n1, "cpus", 4, "memoryMb", 4096))))
                .andExpect(status().isOk());
        String j = "web-j-" + UUID.randomUUID();
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("jobId", j, "name", "单作业",
                                "cpus", 2, "durationLimitMs", 1000, "retryBackoffMs", 1))))
                .andExpect(jsonPath("$.job.state").value("RUNNING"));

        // 为在共享上下文里确定性验证 NO_ONLINE_NODE，先把集群内其它在线节点临时下线
        JsonNode snap = M.readTree(mvc.perform(get("/api/snapshot")).andReturn()
                .getResponse().getContentAsString());
        java.util.List<String> othersDown = new java.util.ArrayList<>();
        for (JsonNode n : snap.get("nodes")) {
            String id = n.get("nodeId").asText();
            if (!id.equals(n1) && "UP".equals(n.get("state").asText())) {
                mvc.perform(post("/api/nodes/" + id + "/down?lost=false")).andExpect(status().isOk());
                othersDown.add(id);
            }
        }

        // 节点失联
        mvc.perform(post("/api/nodes/" + n1 + "/down?lost=true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("LOST"));
        JsonNode job = M.readTree(mvc.perform(get("/api/jobs/" + j))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        log.info("[判定依据] 节点失联后作业状态={}，原因={}",
                job.get("state").asText(), job.get("reasonDetail").asText());
        org.junit.jupiter.api.Assertions.assertTrue(
                job.get("state").asText().equals("RETRY_WAIT")
                        || job.get("state").asText().equals("QUEUED"));

        // 退避后手动调度：无在线节点 -> 排队原因 NO_ONLINE_NODE
        Thread.sleep(5);
        mvc.perform(post("/api/schedule")).andExpect(status().isOk());
        job = M.readTree(mvc.perform(get("/api/jobs/" + j)).andReturn()
                .getResponse().getContentAsString());
        org.junit.jupiter.api.Assertions.assertEquals("QUEUED", job.get("state").asText());
        org.junit.jupiter.api.Assertions.assertEquals("NO_ONLINE_NODE",
                job.get("queueReason").asText());

        // 节点恢复 -> 重新派发；其它节点一并恢复（保持上下文干净）
        mvc.perform(post("/api/nodes/" + n1 + "/recover")).andExpect(status().isOk());
        for (String id : othersDown) {
            mvc.perform(post("/api/nodes/" + id + "/recover")).andExpect(status().isOk());
        }
        mvc.perform(get("/api/jobs/" + j))
                .andExpect(jsonPath("$.state").value("RUNNING"));
        log.info("[判定依据] 节点恢复后退避作业重新 RUNNING（整组重试）");
    }
}

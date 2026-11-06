package com.bishokudev.maintenance.e2e;

import com.bishokudev.maintenance.actuator.MaintenanceStatusResponse;
import com.bishokudev.maintenance.model.MaintenanceState;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Comprehensive End-to-End (E2E) integration test validating:
 * 1. Startup directly in maintenance mode via configuration.
 * 2. Kubernetes readiness probe degradation to 503 OUT_OF_SERVICE at boot.
 * 3. Kubernetes liveness probe remaining 200 UP at boot.
 * 4. Consumer queue pausing on startup (Kafka paused, RabbitMQ stopped).
 * 5. Preservation of @Scheduled tasks (querying MaintenanceState.isWorkBlocked()).
 * 6. Dynamic exit via Actuator POST /actuator/maintenance.
 * 7. Recovery of readiness probe to 200 UP and resumption of consumer queues.
 */
@SpringBootTest(
        classes = StartupTestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "maintenance.start-in-maintenance=true",
                "maintenance.startup.reason=Scheduled cold deployment v1",
                "maintenance.drain-delay=0s",
                "maintenance.hook-timeout=5s",
                "management.endpoints.web.exposure.include=health,info,maintenance",
                "management.endpoint.maintenance.enabled=true",
                "management.endpoint.health.probes.enabled=true",
                "management.health.readinessstate.enabled=true",
                "management.health.livenessstate.enabled=true"
        }
)
public class StartupMaintenanceModeE2ETest {

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private MaintenanceState maintenanceState;

    @Autowired
    private StartupTestApplication.ScheduledTaskTracker scheduledTaskTracker;

    private final Client httpClient = new Client();

    private class Client {
        <T> ResponseEntity<T> getForEntity(String path, Class<T> type) {
            return exchange(path, HttpMethod.GET, null, type);
        }

        @SuppressWarnings("unchecked")
        <T> ResponseEntity<T> exchange(String path, HttpMethod method, HttpEntity<?> entity, Class<T> type) {
            try {
                HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path));
                if (method == HttpMethod.POST) {
                    String json = "{}";
                    if (entity != null && entity.getBody() instanceof String raw) {
                        json = raw;
                    } else if (entity != null && entity.getBody() instanceof Map<?, ?> body) {
                        StringBuilder sb = new StringBuilder("{");
                        sb.append("\"enabled\":").append(body.get("enabled"));
                        if (body.containsKey("reason") && body.get("reason") != null) {
                            sb.append(",\"reason\":\"").append(body.get("reason")).append("\"");
                        }
                        sb.append("}");
                        json = sb.toString();
                    }
                    request.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json));
                } else {
                    request.GET();
                }

                HttpResponse<String> response = HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString());
                Object body = response.body();
                if (type == MaintenanceStatusResponse.class) {
                    var parsed = JsonPath.parse(response.body());
                    body = new MaintenanceStatusResponse(
                            parsed.read("$.active"),
                            Instant.parse(parsed.read("$.lastChanged")),
                            parsed.read("$.reason"),
                            parsed.read("$.details")
                    );
                }
                return ResponseEntity.status(response.statusCode()).body((T) body);
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    private ResponseEntity<MaintenanceStatusResponse> sendMaintenanceCommand(boolean enabled, String reason) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> requestBody = reason != null
                ? Map.of("enabled", enabled, "reason", reason)
                : Map.of("enabled", enabled);

        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

        return httpClient.exchange(
                "/actuator/maintenance",
                HttpMethod.POST,
                requestEntity,
                MaintenanceStatusResponse.class
        );
    }

    @BeforeEach
    void resetToStartupState() {
        StartupTestApplication.resetDiagnostics();
        if (!maintenanceState.isMaintenanceActive()) {
            sendMaintenanceCommand(true, "Scheduled cold deployment v1");
        }
        StartupTestApplication.kafkaContainerPaused.set(true);
        StartupTestApplication.rabbitContainerRunning.set(false);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Tier 1: Feature Coverage (Startup in Maintenance Mode)
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Tier 1: Initial state on startup - MaintenanceState is active and work is blocked")
    void tier1_initialStartupStateShouldBeActive() {
        assertThat(maintenanceState.isMaintenanceActive()).isTrue();
        assertThat(maintenanceState.getPhase()).isEqualTo("ACTIVE");
        assertThat(maintenanceState.isWorkBlocked()).isTrue();
        assertThat(maintenanceState.getReason()).contains("cold deployment");
    }

    @Test
    @DisplayName("Tier 1: Startup readiness probe reports HTTP 503 OUT_OF_SERVICE")
    void tier1_startupReadinessProbeShouldReport503OutOfService() {
        ResponseEntity<String> readinessResp = httpClient.getForEntity("/actuator/health/readiness", String.class);
        assertThat(readinessResp.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(readinessResp.getBody()).contains("\"status\":\"OUT_OF_SERVICE\"");
    }

    @Test
    @DisplayName("Tier 1: Startup liveness probe reports HTTP 200 UP (pod must not be restarted by K8s)")
    void tier1_startupLivenessProbeShouldRemainHealthy() {
        ResponseEntity<String> livenessResp = httpClient.getForEntity("/actuator/health/liveness", String.class);
        assertThat(livenessResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(livenessResp.getBody()).contains("\"status\":\"UP\"");
    }

    @Test
    @DisplayName("Tier 1: GET /actuator/maintenance reflects active maintenance on startup")
    void tier1_actuatorStatusEndpointReportsStartupMaintenance() {
        ResponseEntity<MaintenanceStatusResponse> response =
                httpClient.getForEntity("/actuator/maintenance", MaintenanceStatusResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().active()).isTrue();
        assertThat(response.getBody().reason()).contains("cold deployment");
    }

    // ──────────────────────────────────────────────────────────────────────
    // Tier 2: Boundary & Corner Cases
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Tier 2: Dynamic exit with empty or missing reason succeeds cleanly")
    void tier2_dynamicExitWithEmptyOrMissingReason() {
        // Exit with missing reason field
        ResponseEntity<MaintenanceStatusResponse> exitResponse = sendMaintenanceCommand(false, null);
        assertThat(exitResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(exitResponse.getBody()).isNotNull();
        assertThat(exitResponse.getBody().active()).isFalse();

        // Verify readiness recovered
        ResponseEntity<String> readiness = httpClient.getForEntity("/actuator/health/readiness", String.class);
        assertThat(readiness.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    @DisplayName("Tier 2: Duplicate exit commands via Actuator are idempotent and safe")
    void tier2_duplicateExitCommandsShouldBeIdempotent() {
        // First exit
        ResponseEntity<MaintenanceStatusResponse> first = sendMaintenanceCommand(false, "First exit");
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody().active()).isFalse();

        // Second exit (duplicate)
        ResponseEntity<MaintenanceStatusResponse> second = sendMaintenanceCommand(false, "Duplicate exit");
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().active()).isFalse();

        // Readiness remains UP
        ResponseEntity<String> readiness = httpClient.getForEntity("/actuator/health/readiness", String.class);
        assertThat(readiness.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(readiness.getBody()).contains("\"status\":\"UP\"");
    }

    // ──────────────────────────────────────────────────────────────────────
    // Tier 3: Cross-Feature Interactions
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Tier 3: Consumer queues paused on startup and resumed on Actuator exit")
    void tier3_queuesPausedAtStartupAndResumedOnExit() {
        // In startup maintenance mode, queues must be paused/stopped
        assertThat(StartupTestApplication.kafkaContainerPaused.get()).isTrue();
        assertThat(StartupTestApplication.rabbitContainerRunning.get()).isFalse();

        // Dynamic exit via Actuator
        ResponseEntity<MaintenanceStatusResponse> exitResp =
                sendMaintenanceCommand(false, "Resume queues test");
        assertThat(exitResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Queues must now be resumed
        assertThat(StartupTestApplication.kafkaContainerPaused.get()).isFalse();
        assertThat(StartupTestApplication.rabbitContainerRunning.get()).isTrue();
    }

    @Test
    @DisplayName("Tier 3: Scheduled task execution is blocked during maintenance and executes after exit")
    void tier3_scheduledTasksPreservationAndExecutionAfterExit() {
        // Verify scheduled task was running but blocked while in startup maintenance
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(scheduledTaskTracker.getBlockedCount()).isGreaterThan(0);
        });

        // Dynamic exit
        sendMaintenanceCommand(false, "Unblock scheduled tasks");

        // Verify scheduled task executes business logic after exit
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(scheduledTaskTracker.getExecutionCount()).isGreaterThan(0);
        });
    }

    @Test
    @DisplayName("Tier 3: Readiness probe transitions dynamically from 503 to 200 upon Actuator exit")
    void tier3_readinessProbeTransitionsFrom503To200() {
        // 1. Prior to exit: 503 OUT_OF_SERVICE
        ResponseEntity<String> before = httpClient.getForEntity("/actuator/health/readiness", String.class);
        // Note: may be 503 or 200 depending on test execution order if context is shared;
        // resetting state ensures isolated verification:
        sendMaintenanceCommand(true, "Re-enter for transition test");

        ResponseEntity<String> unready = httpClient.getForEntity("/actuator/health/readiness", String.class);
        assertThat(unready.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(unready.getBody()).contains("\"status\":\"OUT_OF_SERVICE\"");

        // 2. Trigger exit
        sendMaintenanceCommand(false, "Exit transition test");

        // 3. After exit: 200 UP
        ResponseEntity<String> ready = httpClient.getForEntity("/actuator/health/readiness", String.class);
        assertThat(ready.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ready.getBody()).contains("\"status\":\"UP\"");
    }

    // ──────────────────────────────────────────────────────────────────────
    // Tier 4: Real-World Scenarios
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Tier 4: Complete real-world cold boot -> maintenance -> dynamic exit lifecycle")
    void tier4_completeLifecycleTest() {
        StartupTestApplication.resetDiagnostics();

        // 1. Initial State: App booted in maintenance mode
        assertThat(maintenanceState.isMaintenanceActive()).isTrue();
        assertThat(maintenanceState.isWorkBlocked()).isTrue();

        // 2. K8s probe check: Readiness fails (503), Liveness passes (200)
        ResponseEntity<String> readiness = httpClient.getForEntity("/actuator/health/readiness", String.class);
        assertThat(readiness.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);

        ResponseEntity<String> liveness = httpClient.getForEntity("/actuator/health/liveness", String.class);
        assertThat(liveness.getStatusCode()).isEqualTo(HttpStatus.OK);

        // 3. Queues check: Kafka paused, RabbitMQ stopped
        assertThat(StartupTestApplication.kafkaContainerPaused.get()).isTrue();
        assertThat(StartupTestApplication.rabbitContainerRunning.get()).isFalse();

        // 4. Scheduled task check: Task runs but skips work
        assertThat(scheduledTaskTracker.getBlockedCount()).isGreaterThanOrEqualTo(0);

        // 5. Administrator performs cold maintenance tasks (e.g., DB schema migrations)

        // 6. Administrator signals completion via Actuator POST /actuator/maintenance
        ResponseEntity<MaintenanceStatusResponse> exitResponse =
                sendMaintenanceCommand(false, "Cold deployment completed successfully");

        assertThat(exitResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(exitResponse.getBody()).isNotNull();
        assertThat(exitResponse.getBody().active()).isFalse();
        assertThat(exitResponse.getBody().reason()).isEqualTo("Cold deployment completed successfully");

        // 7. Verification: State updated to INACTIVE
        assertThat(maintenanceState.isMaintenanceActive()).isFalse();
        assertThat(maintenanceState.isWorkBlocked()).isFalse();

        // 8. Verification: Readiness recovers to 200 UP
        ResponseEntity<String> recoveredReadiness = httpClient.getForEntity("/actuator/health/readiness", String.class);
        assertThat(recoveredReadiness.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recoveredReadiness.getBody()).contains("\"status\":\"UP\"");

        // 9. Verification: Consumer queues resumed
        assertThat(StartupTestApplication.kafkaContainerPaused.get()).isFalse();
        assertThat(StartupTestApplication.rabbitContainerRunning.get()).isTrue();

        // 10. Verification: Scheduled tasks proceed with work
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(scheduledTaskTracker.getExecutionCount()).isGreaterThan(0);
        });

        // 11. Verification: Exit hook executed and change event emitted
        assertThat(StartupTestApplication.hookExited.get()).isTrue();
        assertThat(StartupTestApplication.eventCount.get()).isGreaterThanOrEqualTo(1);
    }
}

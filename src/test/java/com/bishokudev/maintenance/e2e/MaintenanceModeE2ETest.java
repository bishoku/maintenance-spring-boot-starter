package com.bishokudev.maintenance.e2e;

import com.bishokudev.maintenance.actuator.MaintenanceStatusResponse;
import com.bishokudev.maintenance.example.TestApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-End (E2E) integration test running against a live embedded web server on a random port.
 * Tests real HTTP calls to /actuator/maintenance and validates Kubernetes readiness probe degradation.
 */
@SpringBootTest(
        classes = TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@ActiveProfiles("e2e")
class MaintenanceModeE2ETest {

    @Autowired
    private TestRestTemplate restTemplate;

    @BeforeEach
    void resetState() {
        TestApplication.hookEntered.set(false);
        TestApplication.hookExited.set(false);
        TestApplication.eventCount.set(0);

        // Ensure we start with maintenance mode OFF
        sendMaintenanceCommand(false, "Reset before test");
    }

    @Test
    @DisplayName("Initial State: Application is UP and Readiness probe is ACCEPTING_TRAFFIC (200 OK)")
    void initialProbeStateShouldBeAcceptingTraffic() {
        ResponseEntity<String> readinessResp = restTemplate.getForEntity("/actuator/health/readiness", String.class);
        assertThat(readinessResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(readinessResp.getBody()).contains("\"status\":\"UP\"");

        ResponseEntity<MaintenanceStatusResponse> maintenanceResp =
                restTemplate.getForEntity("/actuator/maintenance", MaintenanceStatusResponse.class);
        assertThat(maintenanceResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(maintenanceResp.getBody()).isNotNull();
        assertThat(maintenanceResp.getBody().active()).isFalse();
    }

    @Test
    @DisplayName("E2E Maintenance Lifecycle: Enter maintenance -> Readiness fails (503) -> Exit maintenance -> Readiness passes (200)")
    void fullMaintenanceLifecycleE2E() {
        // 1. Initial health readiness check
        ResponseEntity<String> initialReadiness = restTemplate.getForEntity("/actuator/health/readiness", String.class);
        assertThat(initialReadiness.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(initialReadiness.getBody()).contains("\"status\":\"UP\"");

        // 2. Trigger Enter Maintenance via HTTP POST /actuator/maintenance
        ResponseEntity<MaintenanceStatusResponse> enterResponse =
                sendMaintenanceCommand(true, "Database Schema Migration v2.5");

        assertThat(enterResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(enterResponse.getBody()).isNotNull();
        assertThat(enterResponse.getBody().active()).isTrue();
        assertThat(enterResponse.getBody().reason()).isEqualTo("Database Schema Migration v2.5");
        assertThat(enterResponse.getBody().details()).containsEntry("readiness", "REFUSING_TRAFFIC");

        // Verify Custom Hook executed
        assertThat(TestApplication.hookEntered.get()).isTrue();
        assertThat(TestApplication.eventCount.get()).isGreaterThanOrEqualTo(1);

        // 3. Verify Kubernetes Readiness Probe is now FAILING (503 Service Unavailable / OUT_OF_SERVICE)
        ResponseEntity<String> refusedReadiness = restTemplate.getForEntity("/actuator/health/readiness", String.class);
        assertThat(refusedReadiness.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(refusedReadiness.getBody()).contains("\"status\":\"OUT_OF_SERVICE\"");

        // 4. Verify Liveness Probe is STILL HEALTHY (200 OK) -> Kubernetes will NEVER kill the pod!
        ResponseEntity<String> livenessResp = restTemplate.getForEntity("/actuator/health/liveness", String.class);
        assertThat(livenessResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(livenessResp.getBody()).contains("\"status\":\"UP\"");

        // 5. Test Idempotency over HTTP (calling enter again does not throw error)
        ResponseEntity<MaintenanceStatusResponse> duplicateEnter =
                sendMaintenanceCommand(true, "Duplicate request");
        assertThat(duplicateEnter.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(duplicateEnter.getBody()).isNotNull();
        assertThat(duplicateEnter.getBody().active()).isTrue();

        // 6. Trigger Exit Maintenance via HTTP POST /actuator/maintenance
        ResponseEntity<MaintenanceStatusResponse> exitResponse =
                sendMaintenanceCommand(false, "Migration completed successfully");

        assertThat(exitResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(exitResponse.getBody()).isNotNull();
        assertThat(exitResponse.getBody().active()).isFalse();
        assertThat(exitResponse.getBody().reason()).isEqualTo("Migration completed successfully");

        // Verify Exit Hook executed
        assertThat(TestApplication.hookExited.get()).isTrue();

        // 7. Verify Kubernetes Readiness Probe is back to 200 OK (ACCEPTING_TRAFFIC / UP)
        ResponseEntity<String> restoredReadiness = restTemplate.getForEntity("/actuator/health/readiness", String.class);
        assertThat(restoredReadiness.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(restoredReadiness.getBody()).contains("\"status\":\"UP\"");
    }

    private ResponseEntity<MaintenanceStatusResponse> sendMaintenanceCommand(boolean enabled, String reason) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> requestBody = Map.of(
                "enabled", enabled,
                "reason", reason != null ? reason : ""
        );

        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

        return restTemplate.exchange(
                "/actuator/maintenance",
                HttpMethod.POST,
                requestEntity,
                MaintenanceStatusResponse.class
        );
    }
}

package com.bishokudev.maintenance.adversarial;

import com.bishokudev.maintenance.e2e.StartupTestApplication;
import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Adversarial stress harness verifying Kubernetes readiness probe transitions,
 * probe stability under high concurrent query load, and resistance to rogue/external
 * AvailabilityChangeEvent spam.
 */
@SpringBootTest(
        classes = StartupTestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "maintenance.start-in-maintenance=true",
                "maintenance.startup.reason=Stress testing probe transitions",
                "maintenance.drain-delay=0s",
                "management.endpoints.web.exposure.include=health,info,maintenance",
                "management.endpoint.maintenance.enabled=true",
                "management.endpoint.health.probes.enabled=true",
                "management.health.readinessstate.enabled=true",
                "management.health.livenessstate.enabled=true"
        }
)
public class ReadinessProbeConcurrentStressTest {

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private MaintenanceState maintenanceState;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    private KubernetesReadinessManager readinessManager;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        return httpClient.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postMaintenance(boolean enabled, String reason) throws Exception {
        String json = "{\"enabled\":" + enabled + ",\"reason\":\"" + reason + "\"}";
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/actuator/maintenance"))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return httpClient.send(req, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @DisplayName("Stress: High concurrent query load on /actuator/health/readiness during active startup maintenance")
    void highConcurrentQueriesDuringActiveStartupMaintenance() throws Exception {
        int concurrency = 25;
        int requestsPerThread = 20;
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(concurrency);

        AtomicInteger status503Count = new AtomicInteger(0);
        AtomicInteger otherStatusCount = new AtomicInteger(0);

        for (int i = 0; i < concurrency; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int j = 0; j < requestsPerThread; j++) {
                        HttpResponse<String> resp = get("/actuator/health/readiness");
                        if (resp.statusCode() == HttpStatus.SERVICE_UNAVAILABLE.value() &&
                                resp.body().contains("\"status\":\"OUT_OF_SERVICE\"")) {
                            status503Count.incrementAndGet();
                        } else {
                            otherStatusCount.incrementAndGet();
                        }
                    }
                } catch (Exception ex) {
                    otherStatusCount.incrementAndGet();
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        boolean finished = endGate.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(finished).isTrue();
        assertThat(otherStatusCount.get()).isEqualTo(0);
        assertThat(status503Count.get()).isEqualTo(concurrency * requestsPerThread);
    }

    @Test
    @DisplayName("Stress: Readiness probe transition under continuous concurrent load with dynamic Actuator exit")
    void readinessProbeTransitionUnderConcurrentLoad() throws Exception {
        // Ensure active state
        if (!maintenanceState.isMaintenanceActive()) {
            postMaintenance(true, "Re-enter before stress transition");
        }

        int readerThreads = 15;
        ExecutorService pool = Executors.newFixedThreadPool(readerThreads + 1);
        AtomicBoolean running = new AtomicBoolean(true);
        List<Integer> observedStatuses = new CopyOnWriteArrayList<>();
        AtomicInteger errorCount = new AtomicInteger(0);

        CountDownLatch readersStarted = new CountDownLatch(readerThreads);

        // Spin up reader threads hitting readiness continuously
        for (int i = 0; i < readerThreads; i++) {
            pool.submit(() -> {
                readersStarted.countDown();
                while (running.get()) {
                    try {
                        HttpResponse<String> resp = get("/actuator/health/readiness");
                        observedStatuses.add(resp.statusCode());
                        if (resp.statusCode() != 503 && resp.statusCode() != 200) {
                            errorCount.incrementAndGet();
                        }
                    } catch (Exception ex) {
                        errorCount.incrementAndGet();
                    }
                }
            });
        }

        readersStarted.await();

        // Let readers hit 503 a few times
        Thread.sleep(100);

        // Trigger dynamic exit via Actuator
        HttpResponse<String> exitResponse = postMaintenance(false, "Dynamic exit under concurrent load");
        assertThat(exitResponse.statusCode()).isEqualTo(200);

        // Wait until readiness is 200
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            HttpResponse<String> current = get("/actuator/health/readiness");
            assertThat(current.statusCode()).isEqualTo(200);
            assertThat(current.body()).contains("\"status\":\"UP\"");
        });

        // Let readers hit 200 a few times
        Thread.sleep(100);
        running.set(false);
        pool.shutdown();
        pool.awaitTermination(3, TimeUnit.SECONDS);

        assertThat(errorCount.get()).isEqualTo(0);
        assertThat(observedStatuses).contains(503);
        assertThat(observedStatuses).contains(200);
    }

    @Test
    @DisplayName("Adversarial: Rogue ACCEPTING_TRAFFIC events while maintenance is active are strictly intercepted")
    void rogueAcceptingTrafficEventsAreInterceptedWhenActive() throws Exception {
        // Ensure active state
        if (!maintenanceState.isMaintenanceActive()) {
            postMaintenance(true, "Re-enter for rogue event test");
        }

        int spammerThreads = 10;
        int spamsPerThread = 20;
        ExecutorService pool = Executors.newFixedThreadPool(spammerThreads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(spammerThreads);

        for (int i = 0; i < spammerThreads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int j = 0; j < spamsPerThread; j++) {
                        // Attempt to falsely advertise ACCEPTING_TRAFFIC
                        AvailabilityChangeEvent.publish(eventPublisher, this, ReadinessState.ACCEPTING_TRAFFIC);
                        Thread.yield();
                    }
                } catch (Exception ex) {
                    // ignore
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        endGate.await(5, TimeUnit.SECONDS);
        pool.shutdown();

        // Despite spam of ACCEPTING_TRAFFIC, MaintenanceStartupInitializer must have re-enforced REFUSING_TRAFFIC
        HttpResponse<String> finalReadiness = get("/actuator/health/readiness");
        assertThat(finalReadiness.statusCode()).isEqualTo(503);
        assertThat(finalReadiness.body()).contains("\"status\":\"OUT_OF_SERVICE\"");
    }
}

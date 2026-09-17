package com.bishokudev.maintenance;

import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.manager.TrafficDrainHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class TrafficDrainHandlerTest {

    private KubernetesReadinessManager readinessManager;
    private TrafficDrainHandler handler;

    @BeforeEach
    void setUp() {
        readinessManager = mock(KubernetesReadinessManager.class);
        handler = new TrafficDrainHandler(readinessManager);
    }

    @Test
    @DisplayName("Should refuse traffic and apply drain delay")
    void shouldRefuseTrafficAndDrain() {
        TrafficDrainHandler.Result result = handler.refuseTrafficAndDrain(Duration.ofMillis(10));

        verify(readinessManager).refuseTraffic();
        assertThat(result.readiness()).isEqualTo("REFUSING_TRAFFIC");
        assertThat(result.error()).isNull();
        assertThat(result.drainDelayMs()).isEqualTo(10L);
        assertThat(result.interrupted()).isFalse();
    }

    @Test
    @DisplayName("Should refuse traffic without drain delay when duration is zero")
    void shouldRefuseTrafficWithoutDrainWhenZero() {
        TrafficDrainHandler.Result result = handler.refuseTrafficAndDrain(Duration.ZERO);

        verify(readinessManager).refuseTraffic();
        assertThat(result.readiness()).isEqualTo("REFUSING_TRAFFIC");
        assertThat(result.drainDelayMs()).isNull();
    }

    @Test
    @DisplayName("Should capture error when readiness manager fails")
    void shouldCaptureErrorOnReadinessFailure() {
        doThrow(new RuntimeException("K8s unreachable")).when(readinessManager).refuseTraffic();

        TrafficDrainHandler.Result result = handler.refuseTrafficAndDrain(Duration.ZERO);

        assertThat(result.readiness()).isNull();
        assertThat(result.error()).isEqualTo("K8s unreachable");
    }

    @Test
    @DisplayName("Should accept traffic successfully")
    void shouldAcceptTraffic() {
        TrafficDrainHandler.Result result = handler.acceptTraffic();

        verify(readinessManager).acceptTraffic();
        assertThat(result.readiness()).isEqualTo("ACCEPTING_TRAFFIC");
        assertThat(result.error()).isNull();
    }

    @Test
    @DisplayName("Should capture error when accept traffic fails")
    void shouldCaptureErrorOnAcceptFailure() {
        doThrow(new RuntimeException("K8s error")).when(readinessManager).acceptTraffic();

        TrafficDrainHandler.Result result = handler.acceptTraffic();

        assertThat(result.readiness()).isNull();
        assertThat(result.error()).isEqualTo("K8s error");
    }

    @Test
    @DisplayName("Disabled result should return DISABLED readiness")
    void disabledResultShouldReturnDisabledState() {
        TrafficDrainHandler.Result result = TrafficDrainHandler.Result.disabled();

        assertThat(result.readiness()).isEqualTo("DISABLED");
        assertThat(result.error()).isNull();
        assertThat(result.drainDelayMs()).isNull();
        assertThat(result.interrupted()).isFalse();
    }
}

package com.bishokudev.maintenance;

import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KubernetesReadinessManagerTest {

    @Test
    @DisplayName("refuseTraffic should publish REFUSING_TRAFFIC")
    @SuppressWarnings("unchecked")
    void shouldPublishRefusingTraffic() {
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);
        KubernetesReadinessManager manager = new KubernetesReadinessManager(publisher);

        manager.refuseTraffic();

        ArgumentCaptor<AvailabilityChangeEvent<ReadinessState>> captor =
                ArgumentCaptor.forClass(AvailabilityChangeEvent.class);
        verify(publisher).publishEvent(captor.capture());
        assertThat(captor.getValue().getState()).isEqualTo(ReadinessState.REFUSING_TRAFFIC);
    }

    @Test
    @DisplayName("acceptTraffic should publish ACCEPTING_TRAFFIC")
    @SuppressWarnings("unchecked")
    void shouldPublishAcceptingTraffic() {
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);
        KubernetesReadinessManager manager = new KubernetesReadinessManager(publisher);

        manager.acceptTraffic();

        ArgumentCaptor<AvailabilityChangeEvent<ReadinessState>> captor =
                ArgumentCaptor.forClass(AvailabilityChangeEvent.class);
        verify(publisher).publishEvent(captor.capture());
        assertThat(captor.getValue().getState()).isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);
    }
}

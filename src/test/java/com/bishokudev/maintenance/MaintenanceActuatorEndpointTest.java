package com.bishokudev.maintenance;

import com.bishokudev.maintenance.actuator.MaintenanceActuatorEndpoint;
import com.bishokudev.maintenance.actuator.MaintenanceStatusResponse;
import com.bishokudev.maintenance.manager.MaintenanceCoordinator;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class MaintenanceActuatorEndpointTest {

    private MaintenanceCoordinator coordinator;
    private MaintenanceState state;
    private MaintenanceActuatorEndpoint endpoint;

    @BeforeEach
    void setUp() {
        state = new MaintenanceState();
        coordinator = mock(MaintenanceCoordinator.class);
        when(coordinator.getState()).thenReturn(state);
        endpoint = new MaintenanceActuatorEndpoint(coordinator);
    }

    @Test
    @DisplayName("Read operation should return current state")
    void readOperationReturnsCurrentState() {
        MaintenanceStatusResponse response = endpoint.getStatus();

        assertThat(response).isNotNull();
        assertThat(response.active()).isFalse();
        assertThat(response.reason()).isEmpty();
        assertThat(response.lastChanged()).isNotNull();
        assertThat(response.details()).isEmpty();
    }

    @Test
    @DisplayName("Write operation should invoke coordinator and return updated state")
    void writeOperationInvokesCoordinatorAndReturnsStatus() {
        doAnswer(invocation -> {
            state.transition(true, "Scale down workers", Map.of("step", "test"));
            return true;
        }).when(coordinator).setMaintenanceMode(eq(true), eq("Scale down workers"));

        MaintenanceStatusResponse response = endpoint.setStatus(true, "Scale down workers");

        verify(coordinator).setMaintenanceMode(true, "Scale down workers");
        assertThat(response.active()).isTrue();
        assertThat(response.reason()).isEqualTo("Scale down workers");
        assertThat(response.details()).containsEntry("step", "test");
    }

    @Test
    @DisplayName("Write operation with null reason should not throw")
    void writeOperationWithNullReason() {
        endpoint.setStatus(true, null);
        verify(coordinator).setMaintenanceMode(true, null);
    }
}

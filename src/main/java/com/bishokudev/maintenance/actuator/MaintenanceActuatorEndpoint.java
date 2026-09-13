package com.bishokudev.maintenance.actuator;

import com.bishokudev.maintenance.manager.MaintenanceCoordinator;
import com.bishokudev.maintenance.model.MaintenanceSnapshot;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.lang.Nullable;

import java.util.Objects;

/**
 * Spring Boot Actuator endpoint exposed under {@code /actuator/maintenance}.
 * <p>
 * Disabled by default for security. Enable explicitly via:
 * <pre>
 * management.endpoint.maintenance.enabled=true
 * management.endpoints.web.exposure.include=maintenance
 * </pre>
 */
@Endpoint(id = "maintenance", enableByDefault = false)
public class MaintenanceActuatorEndpoint {

    private final MaintenanceCoordinator coordinator;

    public MaintenanceActuatorEndpoint(MaintenanceCoordinator coordinator) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator must not be null");
    }

    /**
     * Reads current maintenance status.
     *
     * @return current maintenance state response
     */
    @ReadOperation
    public MaintenanceStatusResponse getStatus() {
        MaintenanceSnapshot snap = coordinator.getState().getSnapshot();
        return new MaintenanceStatusResponse(
                snap.active(),
                snap.lastChanged(),
                snap.reason(),
                snap.details()
        );
    }

    /**
     * Toggles maintenance mode.
     *
     * @param enabled {@code true} to enter maintenance; {@code false} to exit
     * @param reason  optional description/reason for the transition
     * @return updated maintenance state response
     */
    @WriteOperation
    public MaintenanceStatusResponse setStatus(boolean enabled, @Nullable String reason) {
        coordinator.setMaintenanceMode(enabled, reason);
        return getStatus();
    }
}

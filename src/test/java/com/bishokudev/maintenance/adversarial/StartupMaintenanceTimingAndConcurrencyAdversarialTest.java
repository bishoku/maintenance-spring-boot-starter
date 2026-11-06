package com.bishokudev.maintenance.adversarial;

import com.bishokudev.maintenance.actuator.MaintenanceActuatorEndpoint;
import com.bishokudev.maintenance.actuator.MaintenanceStatusResponse;
import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.event.MaintenanceModeChangedEvent;
import com.bishokudev.maintenance.hook.MaintenanceHookExecutor;
import com.bishokudev.maintenance.manager.ConsumerLifecycleManager;
import com.bishokudev.maintenance.manager.KubernetesReadinessManager;
import com.bishokudev.maintenance.manager.MaintenanceCoordinator;
import com.bishokudev.maintenance.manager.MaintenanceStartupInitializer;
import com.bishokudev.maintenance.manager.QueueMaintenanceManager;
import com.bishokudev.maintenance.manager.TrafficDrainHandler;
import com.bishokudev.maintenance.model.MaintenanceSnapshot;
import com.bishokudev.maintenance.model.MaintenanceState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ApplicationAvailabilityBean;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Empirical challenger stress test suite investigating:
 * 1. Startup timing, race conditions, and thread-safety of AvailabilityChangeEvent interception.
 * 2. Actuator exit command issued during and immediately after boot.
 * 3. Behavior when maintenance.queues.enabled=false or maintenance.readiness.enabled=false.
 * 4. Consistent @Scheduled execution across multiple toggle cycles.
 */
public class StartupMaintenanceTimingAndConcurrencyAdversarialTest {

    // ──────────────────────────────────────────────────────────────────────
    // 1. AvailabilityChangeEvent Timing & Race Scenarios
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Challenge 1A: Concurrent flood of ACCEPTING_TRAFFIC events during startup/active maintenance")
    void challenge1A_concurrentFloodOfAcceptingTrafficEvents() throws Exception {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup cold deploy"));
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        QueueMaintenanceManager queueManager = mock(QueueMaintenanceManager.class);
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager, publisher);

        int threads = 30;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);

        AvailabilityChangeEvent<ReadinessState> event =
                new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startGate.await();
                    initializer.onApplicationEvent(event);
                    successCount.incrementAndGet();
                } catch (Exception ex) {
                    // unexpected exception
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        boolean completed = endGate.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(successCount.get()).isEqualTo(threads);
        // Every flood of ACCEPTING_TRAFFIC must be intercepted and converted to refuseTraffic()
        verify(readinessManager, times(threads)).refuseTraffic();
        assertThat(state.isMaintenanceActive()).isTrue();
        assertThat(state.getPhase()).isEqualTo("ACTIVE");
    }

    @Test
    @DisplayName("Challenge 1B: ACCEPTING_TRAFFIC fired BEFORE ApplicationStartedEvent (early event)")
    void challenge1B_acceptingTrafficBeforeApplicationStartedEvent() {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Early event"));
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        QueueMaintenanceManager queueManager = mock(QueueMaintenanceManager.class);

        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager);

        // State is active from instantiation, so even if ACCEPTING_TRAFFIC arrives early, it must be intercepted
        initializer.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));

        verify(readinessManager, times(1)).refuseTraffic();
        assertThat(state.isMaintenanceActive()).isTrue();
    }

    @Test
    @DisplayName("Challenge 1C: ACCEPTING_TRAFFIC when start-in-maintenance is false is NOT intercepted")
    void challenge1C_acceptingTrafficWhenStartInMaintenanceDisabled() {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(false);

        MaintenanceState state = new MaintenanceState(); // inactive
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        QueueMaintenanceManager queueManager = mock(QueueMaintenanceManager.class);

        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager);

        initializer.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));

        // When inactive, ACCEPTING_TRAFFIC must be permitted to stand
        verify(readinessManager, never()).refuseTraffic();
    }

    // ──────────────────────────────────────────────────────────────────────
    // 2. Actuator Exit During and Immediately After Boot
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Challenge 2A: Actuator exit immediately after boot completes")
    void challenge2A_actuatorExitImmediatelyAfterBoot() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        ApplicationAvailabilityBean availability = new ApplicationAvailabilityBean();
        context.getBeanFactory().registerSingleton("applicationAvailability", availability);
        context.addApplicationListener(availability);
        context.refresh();

        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);
        props.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Cold boot"));
        KubernetesReadinessManager readinessManager = new KubernetesReadinessManager(context);
        TrafficDrainHandler drainHandler = new TrafficDrainHandler(readinessManager);
        QueueMaintenanceManager queueManager = new QueueMaintenanceManager(List.of());
        MaintenanceHookExecutor hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));

        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, context, props);
        MaintenanceActuatorEndpoint endpoint = new MaintenanceActuatorEndpoint(coordinator);
        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager, context);

        // 1. Boot happens
        SpringApplication app = mock(SpringApplication.class);
        initializer.onApplicationEvent(new ApplicationStartedEvent(app, new String[0], context, Duration.ofMillis(100)));
        initializer.onApplicationEvent(new ApplicationReadyEvent(app, new String[0], context, Duration.ofMillis(200)));
        // Spring Boot ready listener publishes default ACCEPTING_TRAFFIC
        AvailabilityChangeEvent.publish(context, this, ReadinessState.ACCEPTING_TRAFFIC);
        initializer.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));

        // Verify boot state is REFUSING_TRAFFIC
        assertThat(availability.getReadinessState()).isEqualTo(ReadinessState.REFUSING_TRAFFIC);
        assertThat(state.isMaintenanceActive()).isTrue();

        // 2. Actuator exit issued immediately after boot
        MaintenanceStatusResponse exitResponse = endpoint.setStatus(false, "Dynamic exit post-boot");

        assertThat(exitResponse.active()).isFalse();
        assertThat(state.isMaintenanceActive()).isFalse();
        assertThat(state.getPhase()).isEqualTo("INACTIVE");
        assertThat(state.isWorkBlocked()).isFalse();
        assertThat(availability.getReadinessState()).isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);

        context.close();
    }

    @Test
    @DisplayName("Challenge 2B: Actuator exit issued DURING boot (between Started and Ready events)")
    void challenge2B_actuatorExitDuringBootSequence() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        ApplicationAvailabilityBean availability = new ApplicationAvailabilityBean();
        context.getBeanFactory().registerSingleton("applicationAvailability", availability);
        context.addApplicationListener(availability);
        context.refresh();

        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);
        props.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Cold boot"));
        KubernetesReadinessManager readinessManager = new KubernetesReadinessManager(context);
        TrafficDrainHandler drainHandler = new TrafficDrainHandler(readinessManager);
        QueueMaintenanceManager queueManager = new QueueMaintenanceManager(List.of());
        MaintenanceHookExecutor hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));

        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, context, props);
        MaintenanceActuatorEndpoint endpoint = new MaintenanceActuatorEndpoint(coordinator);
        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager, context);

        SpringApplication app = mock(SpringApplication.class);

        // 1. ApplicationStartedEvent fires
        initializer.onApplicationEvent(new ApplicationStartedEvent(app, new String[0], context, Duration.ofMillis(50)));
        assertThat(availability.getReadinessState()).isEqualTo(ReadinessState.REFUSING_TRAFFIC);

        // 2. An early runner or early HTTP call executes Actuator exit BEFORE ApplicationReadyEvent
        MaintenanceStatusResponse exitResp = endpoint.setStatus(false, "Early exit during runner");
        assertThat(exitResp.active()).isFalse();
        assertThat(state.getPhase()).isEqualTo("INACTIVE");
        assertThat(availability.getReadinessState()).isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);

        // 3. ApplicationReadyEvent and Spring Boot's ACCEPTING_TRAFFIC fire after the exit
        initializer.onApplicationEvent(new ApplicationReadyEvent(app, new String[0], context, Duration.ofMillis(100)));
        AvailabilityChangeEvent.publish(context, this, ReadinessState.ACCEPTING_TRAFFIC);

        // Even though ApplicationReadyEvent ran, the final state and readiness must remain ACCEPTING_TRAFFIC!
        assertThat(state.isMaintenanceActive()).isFalse();
        assertThat(state.getPhase()).isEqualTo("INACTIVE");
        assertThat(state.isWorkBlocked()).isFalse();
        assertThat(availability.getReadinessState()).isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);

        context.close();
    }

    @Test
    @DisplayName("Challenge 2D: Post-exit ApplicationReadyEvent does NOT revert readiness to REFUSING_TRAFFIC if already INACTIVE")
    void challenge2D_postExitApplicationReadyEventShouldNotRevertReadiness() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        ApplicationAvailabilityBean availability = new ApplicationAvailabilityBean();
        context.getBeanFactory().registerSingleton("applicationAvailability", availability);
        context.addApplicationListener(availability);
        context.refresh();

        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);
        props.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Cold boot"));
        KubernetesReadinessManager readinessManager = new KubernetesReadinessManager(context);
        TrafficDrainHandler drainHandler = new TrafficDrainHandler(readinessManager);
        QueueMaintenanceManager queueManager = new QueueMaintenanceManager(List.of());
        MaintenanceHookExecutor hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));

        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, context, props);
        MaintenanceActuatorEndpoint endpoint = new MaintenanceActuatorEndpoint(coordinator);
        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager, context);

        SpringApplication app = mock(SpringApplication.class);

        // 1. Cold boot complete
        initializer.onApplicationEvent(new ApplicationStartedEvent(app, new String[0], context, Duration.ofMillis(50)));
        initializer.onApplicationEvent(new ApplicationReadyEvent(app, new String[0], context, Duration.ofMillis(100)));
        AvailabilityChangeEvent.publish(context, this, ReadinessState.ACCEPTING_TRAFFIC);
        initializer.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));
        assertThat(availability.getReadinessState()).isEqualTo(ReadinessState.REFUSING_TRAFFIC);

        // 2. Actuator exit successfully restores ACCEPTING_TRAFFIC
        endpoint.setStatus(false, "Dynamic exit post-boot");
        assertThat(state.getPhase()).isEqualTo("INACTIVE");
        assertThat(availability.getReadinessState()).isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);

        // 3. What if a subsequent ApplicationReadyEvent is emitted while the application is INACTIVE?
        initializer.onApplicationEvent(new ApplicationReadyEvent(app, new String[0], context, Duration.ofMillis(500)));

        // Does readiness revert to REFUSING_TRAFFIC?
        assertThat(availability.getReadinessState()).isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);

        context.close();
    }

    @Test
    @DisplayName("Challenge 2C: High concurrency - 20 simultaneous Actuator exit requests")
    void challenge2C_concurrentActuatorExitRequests() throws Exception {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);
        props.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Cold boot"));
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        TrafficDrainHandler drainHandler = new TrafficDrainHandler(readinessManager);
        QueueMaintenanceManager queueManager = mock(QueueMaintenanceManager.class);
        MaintenanceHookExecutor hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, publisher, props);
        MaintenanceActuatorEndpoint endpoint = new MaintenanceActuatorEndpoint(coordinator);

        int threads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threads);
        AtomicInteger transitionsApplied = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final int id = i;
            executor.submit(() -> {
                try {
                    startGate.await();
                    // Each thread attempts to toggle exit
                    boolean changed = coordinator.setMaintenanceMode(false, "Thread " + id);
                    if (changed) {
                        transitionsApplied.incrementAndGet();
                    }
                } catch (Exception ex) {
                    // unexpected exception
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        boolean completed = endGate.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        // Exactly one thread should succeed with true; others are idempotent no-ops returning false
        assertThat(transitionsApplied.get()).isEqualTo(1);
        assertThat(state.isMaintenanceActive()).isFalse();
        assertThat(state.getPhase()).isEqualTo("INACTIVE");
        assertThat(state.isWorkBlocked()).isFalse();
        verify(readinessManager, times(1)).acceptTraffic();
    }

    // ──────────────────────────────────────────────────────────────────────
    // 3. Properties Disabled: queues.enabled=false and readiness.enabled=false
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Challenge 3A: maintenance.queues.enabled=false - queues untouched, readiness refused on startup, clean exit")
    void challenge3A_queuesDisabledConfiguration() {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);
        props.getQueues().setEnabled(false);
        props.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup without queues"));
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        TrafficDrainHandler drainHandler = new TrafficDrainHandler(readinessManager);

        ConsumerLifecycleManager brokerManager = mock(ConsumerLifecycleManager.class);
        when(brokerManager.name()).thenReturn("mockBroker");
        QueueMaintenanceManager queueManager = new QueueMaintenanceManager(List.of(brokerManager));

        MaintenanceHookExecutor hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, publisher, props);
        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager, publisher);

        // Startup
        initializer.onApplicationEvent(new ApplicationStartedEvent(mock(SpringApplication.class), new String[0], mock(org.springframework.context.ConfigurableApplicationContext.class), Duration.ZERO));
        initializer.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));

        // Broker consumers must NEVER be touched because queues.enabled=false
        verify(brokerManager, never()).stopConsumers();
        // Readiness refusal must still be enforced
        verify(readinessManager, times(2)).refuseTraffic();

        // Dynamic exit
        boolean exited = coordinator.setMaintenanceMode(false, "Dynamic exit");
        assertThat(exited).isTrue();
        assertThat(state.isMaintenanceActive()).isFalse();
        assertThat(state.getPhase()).isEqualTo("INACTIVE");
        // Broker consumers must NOT be resumed because queues.enabled=false
        verify(brokerManager, never()).startConsumers();
        // Readiness must be accepted
        verify(readinessManager, times(1)).acceptTraffic();
        // Report should reflect queues.enabled = false
        assertThat(state.getDetails()).containsKey("queues");
        @SuppressWarnings("unchecked")
        Map<String, Object> queuesDetail = (Map<String, Object>) state.getDetails().get("queues");
        assertThat(queuesDetail).containsEntry("enabled", false);
    }

    @Test
    @DisplayName("Challenge 3B: maintenance.readiness.enabled=false - queues paused, readiness untouched on startup, clean exit")
    void challenge3B_readinessDisabledConfiguration() {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);
        props.getReadiness().setEnabled(false);
        props.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup without readiness"));
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        TrafficDrainHandler drainHandler = new TrafficDrainHandler(readinessManager);

        ConsumerLifecycleManager brokerManager = mock(ConsumerLifecycleManager.class);
        when(brokerManager.name()).thenReturn("mockBroker");
        when(brokerManager.stopConsumers()).thenReturn(Map.of("mockStopped", true));
        when(brokerManager.startConsumers()).thenReturn(Map.of("mockStarted", true));
        QueueMaintenanceManager queueManager = new QueueMaintenanceManager(List.of(brokerManager));

        MaintenanceHookExecutor hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, publisher, props);
        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager, publisher);

        // Startup
        initializer.onApplicationEvent(new ApplicationStartedEvent(mock(SpringApplication.class), new String[0], mock(org.springframework.context.ConfigurableApplicationContext.class), Duration.ZERO));
        initializer.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));

        // Queues must be stopped
        verify(brokerManager, times(1)).stopConsumers();
        // Readiness refusal must NOT be called because readiness is disabled
        verify(readinessManager, never()).refuseTraffic();

        // Dynamic exit
        boolean exited = coordinator.setMaintenanceMode(false, "Dynamic exit");
        assertThat(exited).isTrue();
        assertThat(state.isMaintenanceActive()).isFalse();
        assertThat(state.getPhase()).isEqualTo("INACTIVE");
        // Queues must be resumed
        verify(brokerManager, times(1)).startConsumers();
        // Readiness accept must NOT be called
        verify(readinessManager, never()).acceptTraffic();
        // Report should reflect readiness = DISABLED
        assertThat(state.getDetails()).containsEntry("readiness", "DISABLED");
    }

    @Test
    @DisplayName("Challenge 3C: Both queues and readiness disabled")
    void challenge3C_bothQueuesAndReadinessDisabled() {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);
        props.getQueues().setEnabled(false);
        props.getReadiness().setEnabled(false);
        props.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Bare mode"));
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        TrafficDrainHandler drainHandler = new TrafficDrainHandler(readinessManager);
        QueueMaintenanceManager queueManager = mock(QueueMaintenanceManager.class);
        MaintenanceHookExecutor hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, publisher, props);
        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager, publisher);

        // Startup
        initializer.onApplicationEvent(new ApplicationStartedEvent(mock(SpringApplication.class), new String[0], mock(org.springframework.context.ConfigurableApplicationContext.class), Duration.ZERO));
        initializer.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));

        verify(queueManager, never()).stopConsumers();
        verify(readinessManager, never()).refuseTraffic();
        assertThat(state.isMaintenanceActive()).isTrue();
        assertThat(state.isWorkBlocked()).isTrue();

        // Dynamic exit
        boolean exited = coordinator.setMaintenanceMode(false, "Bare exit");
        assertThat(exited).isTrue();
        assertThat(state.isMaintenanceActive()).isFalse();
        assertThat(state.isWorkBlocked()).isFalse();
    }

    @Test
    @DisplayName("Challenge 3D: Event publishing on startup when queues are disabled (maintenance.queues.enabled=false)")
    void challenge3D_eventPublishingWhenQueuesDisabled() {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);
        props.getQueues().setEnabled(false);
        props.getEvents().setEnabled(true);
        props.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Startup without queues"));
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        QueueMaintenanceManager queueManager = mock(QueueMaintenanceManager.class);
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager, publisher);

        initializer.onApplicationEvent(new ApplicationStartedEvent(
                mock(SpringApplication.class), new String[0], mock(org.springframework.context.ConfigurableApplicationContext.class), Duration.ZERO));

        // When maintenance.events.enabled=true, MaintenanceModeChangedEvent should be published on startup!
        verify(publisher, times(1)).publishEvent(any(MaintenanceModeChangedEvent.class));
    }

    // ──────────────────────────────────────────────────────────────────────
    // 4. Repeated Toggling Cycles & @Scheduled State Consistency
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Challenge 4: Multiple sequential maintenance toggling cycles (start -> exit -> enter -> exit -> enter -> exit)")
    void challenge4_multipleTogglingCyclesAndScheduledConsistency() {
        MaintenanceProperties props = new MaintenanceProperties();
        props.setStartInMaintenance(true);
        props.setDrainDelay(Duration.ZERO);

        MaintenanceState state = new MaintenanceState(MaintenanceSnapshot.activeStartup("Initial startup"));
        KubernetesReadinessManager readinessManager = mock(KubernetesReadinessManager.class);
        TrafficDrainHandler drainHandler = new TrafficDrainHandler(readinessManager);

        AtomicInteger pauseCount = new AtomicInteger(0);
        AtomicInteger resumeCount = new AtomicInteger(0);

        ConsumerLifecycleManager brokerManager = new ConsumerLifecycleManager() {
            @Override public String name() { return "testBroker"; }
            @Override public Map<String, Object> stopConsumers() {
                pauseCount.incrementAndGet();
                return Map.of("paused", true);
            }
            @Override public Map<String, Object> startConsumers() {
                resumeCount.incrementAndGet();
                return Map.of("resumed", true);
            }
        };

        QueueMaintenanceManager queueManager = new QueueMaintenanceManager(List.of(brokerManager));
        MaintenanceHookExecutor hookExecutor = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(5));
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

        MaintenanceCoordinator coordinator = new MaintenanceCoordinator(
                state, drainHandler, queueManager, hookExecutor, publisher, props);
        MaintenanceStartupInitializer initializer = new MaintenanceStartupInitializer(
                props, state, readinessManager, queueManager, publisher);

        // 1. Initial Cold Boot into Maintenance Mode
        initializer.onApplicationEvent(new ApplicationStartedEvent(mock(SpringApplication.class), new String[0], mock(org.springframework.context.ConfigurableApplicationContext.class), Duration.ZERO));
        initializer.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));

        assertThat(state.isMaintenanceActive()).isTrue();
        assertThat(state.isWorkBlocked()).isTrue();
        assertThat(pauseCount.get()).isEqualTo(1);
        assertThat(resumeCount.get()).isEqualTo(0);

        // Run simulated @Scheduled task tick during initial maintenance
        ScheduledWorkerTask task = new ScheduledWorkerTask(state);
        task.tick();
        assertThat(task.getBlockedCount()).isEqualTo(1);
        assertThat(task.getExecutedCount()).isEqualTo(0);

        // Perform 5 alternating cycles
        for (int cycle = 1; cycle <= 5; cycle++) {
            // EXIT
            boolean exited = coordinator.setMaintenanceMode(false, "Exit cycle " + cycle);
            assertThat(exited).isTrue();
            assertThat(state.isMaintenanceActive()).isFalse();
            assertThat(state.isWorkBlocked()).isFalse();
            assertThat(resumeCount.get()).isEqualTo(cycle);

            // Verify scheduled task executes work
            task.tick();
            assertThat(task.getExecutedCount()).isEqualTo(cycle);

            // RE-ENTER
            boolean entered = coordinator.setMaintenanceMode(true, "Enter cycle " + cycle);
            assertThat(entered).isTrue();
            assertThat(state.isMaintenanceActive()).isTrue();
            assertThat(state.isWorkBlocked()).isTrue();
            assertThat(pauseCount.get()).isEqualTo(cycle + 1);

            // Verify scheduled task is blocked
            task.tick();
            assertThat(task.getBlockedCount()).isEqualTo(cycle + 1);
        }

        // Final exit
        boolean finalExit = coordinator.setMaintenanceMode(false, "Final exit");
        assertThat(finalExit).isTrue();
        assertThat(state.isMaintenanceActive()).isFalse();
        assertThat(state.isWorkBlocked()).isFalse();
        task.tick();
        assertThat(task.getExecutedCount()).isEqualTo(6);
    }

    private static class ScheduledWorkerTask {
        private final MaintenanceState maintenanceState;
        private final AtomicInteger blockedCount = new AtomicInteger(0);
        private final AtomicInteger executedCount = new AtomicInteger(0);

        ScheduledWorkerTask(MaintenanceState maintenanceState) {
            this.maintenanceState = maintenanceState;
        }

        void tick() {
            if (maintenanceState.isWorkBlocked()) {
                blockedCount.incrementAndGet();
                return;
            }
            executedCount.incrementAndGet();
        }

        int getBlockedCount() { return blockedCount.get(); }
        int getExecutedCount() { return executedCount.get(); }
    }
}

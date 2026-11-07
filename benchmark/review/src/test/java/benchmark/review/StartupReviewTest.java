package benchmark.review;

import com.bishokudev.maintenance.config.MaintenanceProperties;
import com.bishokudev.maintenance.hook.MaintenanceHookExecutor;
import com.bishokudev.maintenance.manager.*;
import com.bishokudev.maintenance.model.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.properties.bind.*;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

/** Acceptance assertions intentionally expose defects in the published 1.3.0 binary. */
class StartupReviewTest {
    @Test void startupQueueFailureMustBeReportedAsFailed() {
        var properties = new MaintenanceProperties(); properties.setStartInMaintenance(true);
        var state = new MaintenanceState(MaintenanceSnapshot.activeStartup("review"));
        ConsumerLifecycleManager failing = new ConsumerLifecycleManager() {
            public String name() { return "failing"; }
            public Map<String,Object> stopConsumers() { throw new IllegalStateException("review startup stop failure"); }
            public Map<String,Object> startConsumers() { return Map.of(); }
        };
        ApplicationEventPublisher publisher = event -> {};
        var initializer = new MaintenanceStartupInitializer(properties, state,
                new KubernetesReadinessManager(publisher), new QueueMaintenanceManager(List.of(failing)), publisher);
        initializer.onApplicationEvent(mock(ApplicationStartedEvent.class));
        System.out.println("REVIEW_QUEUE_FAILURE phase="+state.getPhase()+" details="+state.getDetails()
                +" reportedPaused="+initializer.isQueuesPausedOnStartup());
        assertThat(state.getPhase()).isEqualTo("FAILED");
        assertThat(state.getDetails()).containsEntry("partialFailure", true);
    }

    @Test void explicitFalseMustOverrideLowerPriorityEnvironmentTrue() {
        var environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new SystemEnvironmentPropertySource("review-env",
                Map.of("MAINTENANCE_START_IN_MAINTENANCE", "true")));
        environment.getPropertySources().addFirst(new MapPropertySource("review-command-line",
                Map.of("maintenance.start-in-maintenance", "false")));
        ConfigurationPropertySources.attach(environment);
        var properties = Binder.get(environment).bind("maintenance", Bindable.of(MaintenanceProperties.class)).get();
        properties.setEnvironment(environment);
        System.out.println("REVIEW_PRECEDENCE effectiveProperty="+environment.getProperty("maintenance.start-in-maintenance")
                +" libraryValue="+properties.isStartInMaintenance());
        assertThat(properties.isStartInMaintenance()).isFalse();
    }

    @Test void actuatorExitMustNotRaceWithStartupQueuePause() throws Exception {
        var properties = new MaintenanceProperties(); properties.setStartInMaintenance(true);
        var state = new MaintenanceState(MaintenanceSnapshot.activeStartup("review"));
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var paused = new AtomicBoolean();
        ConsumerLifecycleManager delayed = new ConsumerLifecycleManager() {
            public String name() { return "delayed"; }
            public Map<String,Object> stopConsumers() {
                entered.countDown();
                try { if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("review barrier timeout"); }
                catch (InterruptedException ex) { throw new IllegalStateException(ex); }
                paused.set(true); return Map.of("paused", true);
            }
            public Map<String,Object> startConsumers() { paused.set(false); return Map.of("resumed", true); }
        };
        var events = new CopyOnWriteArrayList<Object>();
        ApplicationEventPublisher publisher = events::add;
        var readiness = new KubernetesReadinessManager(publisher);
        var queues = new QueueMaintenanceManager(List.of(delayed));
        var hooks = new MaintenanceHookExecutor(List.of(), Duration.ofSeconds(1));
        var coordinator = new MaintenanceCoordinator(state, new TrafficDrainHandler(readiness), queues, hooks, publisher, properties);
        var initializer = new MaintenanceStartupInitializer(properties, state, readiness, queues, publisher);
        var pool = Executors.newFixedThreadPool(2);
        boolean earlyExit;
        try {
            var starting = pool.submit(() -> initializer.onApplicationEvent(mock(ApplicationStartedEvent.class)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var exiting = pool.submit(() -> coordinator.setMaintenanceMode(false, "review early exit"));
            try { exiting.get(300, TimeUnit.MILLISECONDS); earlyExit = true; }
            catch (TimeoutException expected) { earlyExit = false; }
            release.countDown(); starting.get(5, TimeUnit.SECONDS); exiting.get(5, TimeUnit.SECONDS);
            System.out.println("REVIEW_EARLY_EXIT completedBeforePause="+earlyExit+" phase="+state.getPhase()+" consumerPaused="+paused.get());
            assertThat(paused.get()).as("INACTIVE must not retain a paused consumer after successful exit").isFalse();
        } finally { release.countDown(); pool.shutdownNow(); hooks.destroy(); }
    }
}

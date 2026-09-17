package com.bishokudev.maintenance.hook;

import com.bishokudev.maintenance.model.HookExecutionDetail;
import com.bishokudev.maintenance.model.HookExecutionReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes registered {@link MaintenanceHook} callbacks with per-hook timeout enforcement.
 * <p>
 * Hooks are sorted by {@link org.springframework.core.annotation.Order @Order} priority
 * and executed sequentially on a dedicated daemon thread. Each hook is given a maximum
 * duration (configured via {@code maintenance.hook-timeout}) before it is interrupted
 * and reported as timed out.
 * <p>
 * Implements {@link DisposableBean} to ensure the internal thread pool is cleanly shut
 * down when the Spring application context is closed.
 */
public class MaintenanceHookExecutor implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceHookExecutor.class);

    private final List<MaintenanceHook> hooks;
    private final Duration hookTimeout;
    private final ExecutorService executor;

    public MaintenanceHookExecutor(List<MaintenanceHook> hooks, Duration hookTimeout) {
        List<MaintenanceHook> sorted = hooks != null ? new ArrayList<>(hooks) : new ArrayList<>();
        AnnotationAwareOrderComparator.sort(sorted);
        this.hooks = Collections.unmodifiableList(sorted);

        this.hookTimeout = hookTimeout != null ? hookTimeout : Duration.ofSeconds(30);
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "maintenance-hook-executor");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Executes all registered hooks for the given transition direction.
     *
     * @param entering {@code true} if entering maintenance; {@code false} if exiting
     * @return type-safe execution report
     */
    public HookExecutionReport execute(boolean entering) {
        int succeeded = 0;
        int failed = 0;
        int timedOut = 0;
        List<HookExecutionDetail> executions = new ArrayList<>();
        List<String> failures = new ArrayList<>();

        for (MaintenanceHook hook : hooks) {
            String hookName = hook.getClass().getName();
            long hookStart = System.nanoTime();
            try {
                Future<?> future = executor.submit(() -> {
                    if (entering) {
                        hook.onEnterMaintenance();
                    } else {
                        hook.onExitMaintenance();
                    }
                });

                future.get(hookTimeout.toMillis(), TimeUnit.MILLISECONDS);
                long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - hookStart);
                succeeded++;
                executions.add(new HookExecutionDetail(hookName, "success", durationMs));
            } catch (TimeoutException ex) {
                timedOut++;
                long durationMs = hookTimeout.toMillis();
                String msg = String.format("Hook [%s] timed out after %dms", hookName, durationMs);
                log.error(msg);
                failures.add(msg);
                executions.add(new HookExecutionDetail(hookName, "timed_out", durationMs));
            } catch (Exception ex) {
                failed++;
                long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - hookStart);
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                String msg = String.format("Hook [%s] failed: %s", hookName, cause.getMessage());
                log.error("Error executing MaintenanceHook [{}]", hookName, cause);
                failures.add(msg);
                executions.add(new HookExecutionDetail(hookName, "failed", durationMs));
            }
        }

        boolean partialFailure = !failures.isEmpty();
        return new HookExecutionReport(
                hooks.size(), succeeded, failed, timedOut,
                executions, failures, partialFailure
        );
    }

    /**
     * @return unmodifiable, ordered list of registered hooks
     */
    public List<MaintenanceHook> getHooks() {
        return hooks;
    }

    /**
     * Gracefully shuts down the hook executor thread pool.
     */
    @Override
    public void destroy() {
        log.debug("Shutting down maintenance hook executor thread pool.");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                log.warn("Maintenance hook executor did not terminate gracefully; forced shutdown.");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

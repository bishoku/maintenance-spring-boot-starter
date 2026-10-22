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
import java.util.concurrent.FutureTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes registered {@link MaintenanceHook} callbacks with per-hook timeout enforcement.
 * <p>
 * Hooks are sorted by {@link org.springframework.core.annotation.Order @Order} priority
 * and executed sequentially on a dedicated daemon thread. Each hook is given a maximum
 * duration (configured via {@code maintenance.hook-timeout}) before interruption is requested
 * and it is reported as timed out. A hook ignoring interruption keeps subsequent transitions blocked.
 * <p>
 * Implements {@link DisposableBean} to ensure the internal thread pool is cleanly shut
 * down when the Spring application context is closed.
 */
public class MaintenanceHookExecutor implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceHookExecutor.class);

    private final List<MaintenanceHook> hooks;
    private final Duration hookTimeout;
    private final ExecutorService executor;
    private volatile CountDownLatch lastExecution = new CountDownLatch(0);

    public boolean isIdle() { return lastExecution.getCount() == 0; }

    public MaintenanceHookExecutor(List<MaintenanceHook> hooks, Duration hookTimeout) {
        List<MaintenanceHook> sorted = hooks != null ? new ArrayList<>(hooks) : new ArrayList<>();
        AnnotationAwareOrderComparator.sort(sorted);
        this.hooks = Collections.unmodifiableList(sorted);

        this.hookTimeout = hookTimeout != null ? hookTimeout : Duration.ofSeconds(30);
        if (this.hookTimeout.isNegative() || this.hookTimeout.toMillis() < 1)
            throw new IllegalArgumentException("hook-timeout must be at least 1ms");
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
    public synchronized HookExecutionReport execute(boolean entering) {
        int succeeded = 0, failed = 0, timedOut = 0;
        List<HookExecutionDetail> executions = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        if (!isIdle()) {
            return new HookExecutionReport(hooks.size(), 0, 1, 0, List.of(),
                    List.of("Previous timed-out hook is still running; transition remains blocked"), true);
        }
        for (MaintenanceHook hook : hooks) {
            String name = hook.getClass().getName();
            long start = System.nanoTime();
            CountDownLatch completed = new CountDownLatch(1);
            lastExecution = completed;
            FutureTask<Void> task = new FutureTask<>(() -> {
                try {
                    if (entering) hook.onEnterMaintenance(); else hook.onExitMaintenance();
                    return null;
                } finally {
                    // Publish actual completion before FutureTask makes get() return.
                    completed.countDown();
                }
            }) {
                @Override public void run() {
                    try { super.run(); } finally { completed.countDown(); }
                }
            };
            String status = "success";
            try {
                executor.execute(task);
                task.get(hookTimeout.toMillis(), TimeUnit.MILLISECONDS);
                succeeded++;
            } catch (TimeoutException ex) {
                task.cancel(true);
                timedOut++;
                status = "timed_out";
                failures.add("Hook [" + name + "] timed out; cancellation requested");
            } catch (InterruptedException ex) {
                task.cancel(true);
                Thread.currentThread().interrupt();
                failed++;
                status = "interrupted";
                failures.add("Hook execution interrupted: " + name);
            } catch (Exception ex) {
                task.cancel(true);
                if (ex instanceof java.util.concurrent.RejectedExecutionException) completed.countDown();
                failed++;
                status = "failed";
                failures.add("Hook [" + name + "] failed: " + (ex.getCause() == null ? ex : ex.getCause()));
            }
            executions.add(new HookExecutionDetail(name, status,
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)));
            if (!failures.isEmpty()) break;
        }
        return new HookExecutionReport(hooks.size(), succeeded, failed, timedOut,
                executions, failures, !failures.isEmpty());
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

package benchmark;

import com.bishokudev.maintenance.model.MaintenanceState;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.*;

/** Diagnostic fixture only: observes the published starter without changing its lifecycle. */
@RestController
@EnableScheduling
@ConditionalOnProperty(name="benchmark.startup-probe", havingValue="true")
public class StartupProbe {
    private final MaintenanceState state;
    private final AtomicInteger ticks = new AtomicInteger(), blocked = new AtomicInteger(), executed = new AtomicInteger();
    private final AtomicReference<Boolean> firstBlocked = new AtomicReference<>();
    private final Queue<Map<String,Object>> consumed = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean ready = new AtomicBoolean();
    @Value("${benchmark.started-delay-ms:0}") long startedDelayMs;
    public StartupProbe(MaintenanceState state) { this.state = state; }
    @Scheduled(fixedRate=50) public void tick() {
        boolean gated = state.isWorkBlocked();
        firstBlocked.compareAndSet(null, gated);
        ticks.incrementAndGet();
        if (gated) blocked.incrementAndGet(); else executed.incrementAndGet();
    }
    @EventListener @Order(Ordered.HIGHEST_PRECEDENCE)
    public void started(ApplicationStartedEvent event) throws InterruptedException {
        // A bounded, earlier application listener makes the natural pre-initializer window reproducible.
        if (startedDelayMs > 0) Thread.sleep(startedDelayMs);
    }
    public void consumed(String broker, String message) {
        consumed.add(Map.of("broker",broker,"message",message,"workBlocked",state.isWorkBlocked(),"phase",state.getPhase(),"nanos",System.nanoTime()));
    }
    @EventListener public void ready(ApplicationReadyEvent event) { ready.set(true); }
    @GetMapping("/bench/startup") public Map<String,Object> diagnostics() {
        return Map.of("phase",state.getPhase(),"workBlocked",state.isWorkBlocked(),
                "ticks",ticks.get(),"blocked",blocked.get(),"executed",executed.get(),
                "firstTickBlocked",Boolean.TRUE.equals(firstBlocked.get()),"consumed",List.copyOf(consumed),"ready",ready.get());
    }
}

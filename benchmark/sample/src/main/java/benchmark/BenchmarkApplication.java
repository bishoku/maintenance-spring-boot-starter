package benchmark;

import com.bishokudev.maintenance.hook.MaintenanceHook;
import com.bishokudev.maintenance.manager.ConsumerLifecycleManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.web.bind.annotation.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

@SpringBootApplication
@EnableKafka
@EnableRabbit
@RestController
public class BenchmarkApplication {
    final String instance = System.getenv().getOrDefault("POD_NAME", "local");
    final Set<String> kafkaMessages = ConcurrentHashMap.newKeySet();
    final Set<String> rabbitMessages = ConcurrentHashMap.newKeySet();
    final AtomicInteger kafkaInFlight = new AtomicInteger(), rabbitInFlight = new AtomicInteger();
    final AtomicInteger work = new AtomicInteger(), lateHooks = new AtomicInteger(), interruptions = new AtomicInteger();
    volatile String mode = "ok";
    volatile boolean exitSawRunning;
    final KafkaListenerEndpointRegistry kafka;
    final RabbitListenerEndpointRegistry rabbit;
    final KafkaTemplate<String, String> producer;
    final RabbitTemplate rabbitProducer;
    public BenchmarkApplication(KafkaListenerEndpointRegistry kafka, RabbitListenerEndpointRegistry rabbit,
                                KafkaTemplate<String, String> producer, RabbitTemplate rabbitProducer) {
        this.kafka = kafka; this.rabbit = rabbit; this.producer = producer; this.rabbitProducer = rabbitProducer;
    }
    public static void main(String[] args) { SpringApplication.run(BenchmarkApplication.class, args); }
    @Bean NewTopic topic() { return TopicBuilder.name("maintenance-benchmark").partitions(1).replicas(1).build(); }
    @Bean Queue queue() { return new Queue("maintenance-benchmark", false); }
    @KafkaListener(id="activeKafka", topics="maintenance-benchmark", groupId="${benchmark.kafka.group:maintenance-benchmark}")
    void kafka(String message) { consume(message, kafkaInFlight, kafkaMessages); }
    @KafkaListener(id="idleKafka", topics="maintenance-benchmark", groupId="maintenance-benchmark-idle", autoStartup="false")
    void idleKafka(String message) { }
    @RabbitListener(id="activeRabbit", queues="maintenance-benchmark")
    void rabbit(String message) { consume(message, rabbitInFlight, rabbitMessages); }
    void consume(String message, AtomicInteger inFlight, Set<String> messages) {
        inFlight.incrementAndGet();
        try {
            if (mode.equals("broker-slow")) Thread.sleep(1200);
            messages.add(message);
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        finally { inFlight.decrementAndGet(); }
    }
    @RabbitListener(id="idleRabbit", queues="maintenance-benchmark", autoStartup="false")
    void idleRabbit(String message) { }
    @Bean MaintenanceHook testHook() { return new MaintenanceHook() {
        public void onEnterMaintenance() {
            if (mode.equals("enter-fail")) throw new IllegalStateException("injected enter failure");
            if (mode.equals("slow")) {
                try { Thread.sleep(1500); lateHooks.incrementAndGet(); }
                catch (InterruptedException e) { interruptions.incrementAndGet(); Thread.currentThread().interrupt(); }
            }
        }
        public void onExitMaintenance() {
            exitSawRunning = !kafka.getListenerContainer("activeKafka").isContainerPaused()
                    && kafka.getListenerContainer("activeKafka").isRunning();
            if (mode.equals("exit-fail")) throw new IllegalStateException("injected exit failure");
        }
    }; }
    @Bean ConsumerLifecycleManager injectedFailure() { return new ConsumerLifecycleManager() {
        public String name() { return "injected"; }
        public Map<String,Object> stopConsumers() {
            if (mode.equals("queue-fail")) throw new IllegalStateException("injected queue failure");
            return Map.of("ok", true);
        }
        public Map<String,Object> startConsumers() { return Map.of("ok", true); }
    }; }
    @PostMapping("/bench/mode/{value}") void mode(@PathVariable String value) { mode = value; }
    @PostMapping("/bench/send/{id}") void send(@PathVariable String id) throws Exception {
        producer.send("maintenance-benchmark", id).get(10, TimeUnit.SECONDS);
        rabbitProducer.convertAndSend("maintenance-benchmark", id);
    }
    @GetMapping("/bench/state") Map<String,Object> state() {
        var k = kafka.getListenerContainer("activeKafka");
        return Map.of("kafka", Map.of("running", k.isRunning(), "paused", k.isContainerPaused(),
                        "inFlight", kafkaInFlight.get(), "idleRunning", kafka.getListenerContainer("idleKafka").isRunning(), "messages", kafkaMessages),
                "rabbit", Map.of("running", rabbit.getListenerContainer("activeRabbit").isRunning(),
                        "inFlight", rabbitInFlight.get(), "idleRunning", rabbit.getListenerContainer("idleRabbit").isRunning(), "messages", rabbitMessages),
                "instance", instance, "work", work.get(), "lateHooks", lateHooks.get(), "interruptions", interruptions.get(), "exitSawRunning", exitSawRunning);
    }
    @GetMapping("/work") Map<String,Object> work(@RequestParam(defaultValue="0") long ms) throws Exception {
        work.incrementAndGet();
        try { Thread.sleep(Math.min(ms, 30000)); return Map.of("done", true, "instance", instance); }
        finally { work.decrementAndGet(); }
    }
}

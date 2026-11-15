package ro.upb.cebp.workermanager;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import ro.upb.cebp.common.topics.KafkaTopics;

@SpringBootApplication
@EnableScheduling
@OpenAPIDefinition(
        info = @Info(
                title = "CEBP Worker Manager Service API (Member 2)",
                version = "0.1.0",
                description = "Registers workers, monitors 5s heartbeats, batches raw events into analytical jobs, tracks latency/throughput, detects dead workers, manages dynamic scaling, and triggers retries."
        )
)
public class WorkerManagerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerManagerApplication.class, args);
    }

    @Bean
    public NewTopic analyticsJobsTopic() {
        return TopicBuilder.name(KafkaTopics.ANALYTICS_JOBS)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic workerTelemetryTopic() {
        return TopicBuilder.name(KafkaTopics.WORKER_TELEMETRY)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**").allowedOrigins("*").allowedMethods("*");
            }
        };
    }
}

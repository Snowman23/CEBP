package ro.upb.cebp.core;

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
                title = "CEBP Core Management Service API (Member 1)",
                version = "0.1.0",
                description = "Single Source of Truth (SSOT) database owner, Public Transit Ingestion (TfNSW Sydney initial scope, TfL London, HSL Helsinki MQTT, NYC MTA), and AI Operational Proposals registry."
        )
)
public class CoreManagementApplication {

    public static void main(String[] args) {
        SpringApplication.run(CoreManagementApplication.class, args);
    }

    @Bean
    public NewTopic transitEventsRawTopic() {
        return TopicBuilder.name(KafkaTopics.TRANSIT_EVENTS_RAW)
                .partitions(6)
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

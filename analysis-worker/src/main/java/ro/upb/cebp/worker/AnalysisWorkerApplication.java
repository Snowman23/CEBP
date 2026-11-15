package ro.upb.cebp.worker;

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
                title = "CEBP Analysis Worker Pool Service API (Member 3)",
                version = "0.1.0",
                description = "Dynamic Virtual Worker Pool that processes transit analytical tasks (delays, bunching, speed anomalies) and publishes results to Kafka."
        )
)
public class AnalysisWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalysisWorkerApplication.class, args);
    }

    @Bean
    public NewTopic analyticsResultsTopic() {
        return TopicBuilder.name(KafkaTopics.ANALYTICS_RESULTS)
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

package ro.upb.cebp.results;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
@OpenAPIDefinition(
        info = @Info(
                title = "CEBP Analytics Results Service API (Member 3)",
                version = "0.1.0",
                description = "Stores event-derived analytical projections (isolated from core_db) and exposes route delays, anomalies, and SSE live streams via HTTP APIs."
        )
)
public class AnalyticsResultsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsResultsApplication.class, args);
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

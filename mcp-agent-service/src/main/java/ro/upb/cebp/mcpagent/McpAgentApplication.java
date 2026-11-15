package ro.upb.cebp.mcpagent;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestClient;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
@EnableScheduling
@OpenAPIDefinition(
        info = @Info(
                title = "CEBP Autonomous AI Agent Control API (Member 4)",
                version = "0.1.0",
                description = "Controls and inspects the Autonomous Transit & Operations AI Agent. Note: The Remote MCP Server itself is served at `/mcp/sse` and `/mcp/message`."
        )
)
public class McpAgentApplication {

    public static void main(String[] args) {
        SpringApplication.run(McpAgentApplication.class, args);
    }

    @Bean
    public RestClient restClient() {
        return RestClient.create();
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

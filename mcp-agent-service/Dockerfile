FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY pom.xml .
COPY common-events common-events
COPY mcp-agent-service mcp-agent-service
COPY core-management-service/pom.xml core-management-service/pom.xml
COPY worker-manager-service/pom.xml worker-manager-service/pom.xml
COPY analysis-worker/pom.xml analysis-worker/pom.xml
COPY analytics-results-service/pom.xml analytics-results-service/pom.xml
RUN apk add --no-cache maven && mvn -pl common-events,mcp-agent-service -am clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/mcp-agent-service/target/*.jar app.jar
EXPOSE 8084
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

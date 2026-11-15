FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY pom.xml .
COPY common-events common-events
COPY analytics-results-service analytics-results-service
COPY core-management-service/pom.xml core-management-service/pom.xml
COPY worker-manager-service/pom.xml worker-manager-service/pom.xml
COPY analysis-worker/pom.xml analysis-worker/pom.xml
COPY mcp-agent-service/pom.xml mcp-agent-service/pom.xml
RUN apk add --no-cache maven && mvn -pl common-events,analytics-results-service -am clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/analytics-results-service/target/*.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

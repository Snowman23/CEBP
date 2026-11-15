FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY pom.xml .
COPY common-events common-events
COPY worker-manager-service worker-manager-service
COPY core-management-service/pom.xml core-management-service/pom.xml
COPY analysis-worker/pom.xml analysis-worker/pom.xml
COPY analytics-results-service/pom.xml analytics-results-service/pom.xml
COPY mcp-agent-service/pom.xml mcp-agent-service/pom.xml
RUN apk add --no-cache maven && mvn -pl common-events,worker-manager-service -am clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/worker-manager-service/target/*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

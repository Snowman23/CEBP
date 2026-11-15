FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY pom.xml .
COPY common-events common-events
COPY analysis-worker analysis-worker
COPY core-management-service/pom.xml core-management-service/pom.xml
COPY worker-manager-service/pom.xml worker-manager-service/pom.xml
COPY analytics-results-service/pom.xml analytics-results-service/pom.xml
COPY mcp-agent-service/pom.xml mcp-agent-service/pom.xml
RUN apk add --no-cache maven && mvn -pl common-events,analysis-worker -am clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/analysis-worker/target/*.jar app.jar
EXPOSE 8091
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

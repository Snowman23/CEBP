FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY pom.xml .
COPY common-events common-events
COPY core-management-service core-management-service
COPY worker-manager-service/pom.xml worker-manager-service/pom.xml
COPY analysis-worker/pom.xml analysis-worker/pom.xml
COPY analytics-results-service/pom.xml analytics-results-service/pom.xml
COPY mcp-agent-service/pom.xml mcp-agent-service/pom.xml
RUN apk add --no-cache maven && mvn -pl common-events,core-management-service -am clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/core-management-service/target/*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

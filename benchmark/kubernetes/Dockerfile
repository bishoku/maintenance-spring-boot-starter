FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY benchmark.jar /app/benchmark.jar
USER 10001:10001
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=65", "-jar", "/app/benchmark.jar"]

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -DskipTests package


FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/wm-central.jar app.jar
RUN mkdir -p /data
ENV DB_PATH=/data/central.db
EXPOSE 7000
ENTRYPOINT ["java", "-jar", "app.jar"]
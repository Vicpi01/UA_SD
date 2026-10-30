FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY . .
RUN mvn -q -pl WM_WS -am -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/WM_WS/target/wm-ws.jar /app/wm-ws.jar
ENTRYPOINT ["java", "-cp", "/app/wm-ws.jar"]
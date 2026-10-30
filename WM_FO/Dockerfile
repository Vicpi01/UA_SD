FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY . .
RUN mvn -q -pl WM_FO -am -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/WM_FO/target/wm-fo.jar /app/wm-fo.jar
ENTRYPOINT ["java", "-jar", "/app/wm-fo.jar"]
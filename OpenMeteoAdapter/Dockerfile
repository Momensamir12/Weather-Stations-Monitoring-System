FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /workspace

COPY pom.xml .
COPY Common/pom.xml Common/
COPY Bitcask/pom.xml Bitcask/
COPY WeatherStation/pom.xml WeatherStation/
COPY BaseCentralStation/pom.xml BaseCentralStation/
COPY OpenMeteoAdapter/pom.xml OpenMeteoAdapter/
RUN mvn -B -pl OpenMeteoAdapter -am dependency:go-offline

COPY Common Common
COPY OpenMeteoAdapter OpenMeteoAdapter
RUN mvn -B -pl OpenMeteoAdapter -am package -DskipTests

FROM eclipse-temurin:25-jre-alpine
WORKDIR /app

RUN addgroup -S app && adduser -S app -G app
USER app

COPY --from=build /workspace/OpenMeteoAdapter/target/OpenMeteoAdapter-1.0.0.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
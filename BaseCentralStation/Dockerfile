FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /workspace

COPY pom.xml .
COPY Common/pom.xml Common/
COPY Bitcask/pom.xml Bitcask/
COPY WeatherStation/pom.xml WeatherStation/
COPY BaseCentralStation/pom.xml BaseCentralStation/
COPY OpenMeteoAdapter/pom.xml OpenMeteoAdapter/

RUN mvn -B -pl BaseCentralStation -am dependency:go-offline

COPY Common Common
COPY Bitcask Bitcask
COPY BaseCentralStation BaseCentralStation

RUN mvn -B -pl BaseCentralStation -am package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app

RUN groupadd -r app && useradd -r -g app app
RUN mkdir -p /data/bitcask /data/parquet && chown -R app:app /data

USER app

COPY --from=build /workspace/BaseCentralStation/target/BaseCentralStation-1.0.0.jar app.jar

EXPOSE 9090
VOLUME ["/data"]

ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "app.jar"]
# Build stage
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build

# Install Common first
COPY Common ./Common
RUN cd Common && mvn install -DskipTests -B

# Install Bitcask next
COPY Bitcask ./Bitcask
RUN cd Bitcask && mvn install -DskipTests -B

# Build the actual target module
COPY BaseCentralStation ./BaseCentralStation
RUN cd BaseCentralStation && mvn package -DskipTests -B

# Runtime stage
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /build/BaseCentralStation/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
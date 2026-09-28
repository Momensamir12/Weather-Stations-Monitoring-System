# Weather Stations Monitoring System

A distributed, stream-processing pipeline for IoT weather data.

---

## Table of contents

* [Architecture](#architecture)

  * [Weather stations](#weather-stations)
  * [Open-Meteo adapter](#open-meteo-adapter)
  * [Base central station](#base-central-station)
  * [Bitcask](#bitcask)
  * [Data analysis](#data-analysis)
* [Message schema](#message-schema)
* [Build and run](#build-and-run)
* [Kubernetes deployment](#kubernetes-deployment)
* [Kibana analyses](#kibana-analyses)

---

## Architecture

![architecture](architecture.jpg)

The system is a distributed, stream-processing pipeline for IoT weather data.

### Weather stations

Mock weather stations generate weather readings every second and publish them to Kafka. Each station has a unique station ID and randomly generated battery status and weather data. 10% of generated messages are intentionally dropped before publishing to simulate message loss.

### Open-Meteo adapter

The Open-Meteo adapter polls the [Open-Meteo](https://open-meteo.com/) API for current weather data and converts the response into the same Avro message format used by the mock weather stations before publishing it to Kafka.

### Base central station

The base central station is the main processing component of the system. It consumes weather data from Kafka and handles the downstream processing pipeline.

It detects rain events based on humidity, maintains the latest weather status for each station in Bitcask, and archives the complete stream of weather messages into Parquet files for further analysis in Elasticsearch and Kibana.

It also exposes the Bitcask store through a socket server, allowing clients to query the latest weather data for individual stations or all stored stations.

### Bitcask

The system includes a Bitcask-style log-structured key-value store for maintaining the latest weather status of each station.

Records are appended to the active segment file using the binary format `keySize(4) | valueSize(4) | key | value`. When a segment reaches its size limit, a new segment is created.

A background merge process periodically compacts immutable segments by retaining only the latest entry for each key and removing outdated records. The merge also generates `.hint` files containing the key locations in the merged segment. During recovery, these hint files allow the key directory to be rebuilt without scanning the complete data files, reducing startup time.


### Data analysis

All incoming weather messages are archived as Parquet files and later ingested into Elasticsearch. Kibana is used to analyze the collected data, including battery status distribution and dropped messages per station.

## Message schema

Defined in `Common` (`AvroWeatherStatusMessage`):

| Field                       | Type   | Notes                                                  |
| --------------------------- | ------ | ------------------------------------------------------ |
| `stationId`                 | long   | 1–10 for mock stations, 100 for the Open-Meteo adapter |
| `sequenceNumber`            | long   | Increments per message; gaps reveal dropped messages   |
| `batteryStatus`             | enum   | `LOW`, `MID`, `HIGH`, `NOT_AVAILABLE`                  |
| `statusTimeStamp`           | long   | Unix epoch **milliseconds**                            |
| `weatherStatus.humidity`    | int    | Percentage                                             |
| `weatherStatus.temperature` | double | Fahrenheit                                             |
| `weatherStatus.windSpeed`   | double | km/h                                                   |

Rain events (`AvroRainMessage`) contain `stationId` and `humidity`.

## Build and run

**Prerequisites:** JDK 25, Maven 3.9+, Docker, `kubectl`, and a Kubernetes cluster such as Minikube or Kind.

```bash
# Build every module
mvn clean install
```

### Container images

The Docker build context is the repository root because each Dockerfile needs access to the parent POM and shared modules:

```bash
docker build -f WeatherStation/Dockerfile     -t weather-station:latest .
docker build -f OpenMeteoAdapter/Dockerfile   -t open-meteo-adapter:latest .
docker build -f BaseCentralStation/Dockerfile -t base-central-station:latest .
```

## Kubernetes deployment

Clone the repository and move into the project directory:

```bash
git clone https://github.com/Momensamir12/Weather-Stations-Monitoring-System.git
cd Weather-Stations-Monitoring-System
```

Run the deployment script from the repository root:

```bash
./scripts/deploy.sh
```

The script builds the Docker images, loads them into the Kubernetes cluster, deploys Kafka and Schema Registry, and then starts the weather stations, Open-Meteo adapter, and base central station.

## Kibana analyses

The weather data stored in Elasticsearch is analyzed in Kibana to validate the behavior of the simulated weather stations.

### Battery status distribution per station

The battery status distribution approaches the specified 30% low, 40% medium, and 30% high distribution as more messages are collected.

![Battery status distribution](battery-percentage.png)

### Dropped messages per station

10% of generated weather-station messages are intentionally dropped before being published to Kafka. The resulting sequence-number gaps are used to determine the dropped-message rate.

![Dropped messages per station](dropped-messages.png)




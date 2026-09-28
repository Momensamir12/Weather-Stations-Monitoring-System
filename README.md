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

Records are appended to segment files, while background compaction removes outdated entries. Hint files are used to speed up recovery by allowing the key directory to be rebuilt without scanning every record in the data files.

### Data analysis

All incoming weather messages are archived as Parquet files and later ingested into Elasticsearch. Kibana is used to analyze the collected data, including battery status distribution and dropped messages per station.

# Weather Stations Monitoring System

A distributed, stream-processing pipeline for IoT weather data.

---

## Table of contents

* [Architecture](#architecture)
* [Message schema](#message-schema)
* [Components](#components)

  * [Weather station (mock)](#weather-station-mock)
  * [Open-Meteo adapter (bonus)](#open-meteo-adapter-bonus)
  * [Base central station](#base-central-station)
  * [Bitcask](#bitcask)
  * [Parquet archive and Elasticsearch ingestion](#parquet-archive-and-elasticsearch-ingestion)
* [Build and run](#build-and-run)
* [Kubernetes deployment](#kubernetes-deployment)
* [Kibana analyses](#kibana-analyses)

---

## Architecture

![architecture](architecture.jpg)

* Mock weather stations send weather status messages to Kafka every second.
* The Open-Meteo adapter service polls current weather data from the Open-Meteo API and forwards it to Kafka.
* Base central station:

  * detects rain by checking for weather messages with humidity above 70%;
  * maintains an up-to-date view of each station's latest weather status in Bitcask, using the station ID as the key;
  * maintains a history of weather messages by writing all incoming messages to Parquet files, which can then be ingested into Elasticsearch and analyzed in Kibana.

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
docker build -f WeatherStation/Dockerfile      -t weather-station:latest .
docker build -f OpenMeteoAdapter/Dockerfile    -t open-meteo-adapter:latest .
docker build -f BaseCentralStation/Dockerfile  -t base-central-station:latest .
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

The battery status distribution confirms the specified 30% low, 40% medium, and 30% high distribution as more messages are collected.

![Battery status distribution](battery-percentage.png)

### Dropped messages per station

10% of generated weather-station messages are intentionally dropped before being published to Kafka.

![Dropped messages per station](dropped-messages.png)





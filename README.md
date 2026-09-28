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
* [Querying Bitcask](#querying-bitcask)
* [Kibana analyses](#kibana-analyses)

---

## Architecture

![architecture](architecture.jpg)

* Mock weather stations send weather status messages to Kafka every regularly.
* Open-Meteo adapter service polls current weather data from the Open-Meteo API and forwards it to Kafka.
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

## Components

### Weather station 

`WeatherStation` emits a reading every second. Battery status is varied randomly and 10% of generated messages are dropped before publishing to simulate
queryable data which will get ingested by elastic search 


### Open-Meteo adapter

`OpenMeteoAdapter` polls the [Open-Meteo](https://open-meteo.com/) current-weather endpoint for a fixed coordinate and translates the response into the Avro message format used by the mock stations.


### Base central station

The base central station consists of four main services, each handling a different part of the processing pipeline.

KafkaStreamsRainDetectionService uses Kafka Streams to process incoming weather status messages and detect rain events by filtering messages where humidity is above 70%. Detected events are mapped to AvroRainMessage records and published to the rain-events Kafka topic.

BitcaskConsumerService consumes weather status messages from Kafka. For each message, it updates the corresponding station entry in the Bitcask store, using the station ID as the key

ParquetArchiverService writes all received messages to parquet files and partitions them by time and station ID.

BitcaskServer exposes the Bitcask store through a socket server on port 9090. The server handles client requests for reading stored values, allowing the latest weather status for individual stations or all stored keys to be queried remotely.

### Bitcask

Implementation of a Bitcask-style log-structured key-value store.

* **Write path:** Records are appended to the active segment file using the binary format `keySize(4) | valueSize(4) | key | value`.
* **Compaction (merge):** A background process reclaims disk space by removing outdated, overwritten key-value entries from immutable data files.
* **Hint files:** Merged segments receive a `.hint` file, allowing the key directory to be rebuilt during startup by reading small hint files instead of scanning the complete data files.
* **Recovery:** On startup, the key directory is rebuilt from hint files where available and from data files otherwise.

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

The battery status distribution is expected to approach the specified 30% low, 40% medium, and 30% high distribution as more messages are collected.

![Battery status distribution](battery-percentage.png)

### Dropped messages per station

10% of generated weather-station messages are intentionally dropped before they are published to Kafka.

![Dropped messages per station](dropped-messages.png)



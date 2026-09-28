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





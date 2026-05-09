# Real-Time-Ride-Sharing-Data-Platform

##📋 Overview

##🚀 Getting Started
Prerequisites
 1.Docker install and Kafks setup
 ----Create file docker-compose.yml
 ```
version: '3.1'
services:
  zookeeper:
    image: wurstmeister/zookeeper:latest
    container_name: zookeeper
    ports: 
      - "2181:2181" 
    

  kafka:
    image: wurstmeister/kafka:latest
    container_name: kafka
    ports: 
      - "9092:9092" 
    environment:
      KAFKA_ADVERTISED_HOST_NAME: localhost
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```
2.Creating topic
```
docker exec -it <kafka_container> kafka-topics \
--create \
--topic ride_events \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

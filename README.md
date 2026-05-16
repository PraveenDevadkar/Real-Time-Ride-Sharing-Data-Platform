# Real-Time-Ride-Sharing-Data-Platform

##📋 Overview


FINAL PROJECT PROBLEM STATEMENT
🎯 Project Title

Real-Time & Batch E-commerce Data Platform with Cost Monitoring

📌 Problem Statement

An e-commerce company generates large volumes of data from multiple sources such as user orders, product updates, and customer activities. The business requires a scalable data platform that can:

Ingest real-time streaming data (orders, transactions)
Process batch data (daily product/catalog updates)
Maintain historical changes using Slowly Changing Dimensions (SCD Type 2)
Build analytical data models (fact and dimension tables)
Provide monitoring and cost insights dashboards

The system should support both real-time and batch pipelines, ensuring high availability, data accuracy, and scalability.

🧠 OBJECTIVES (What YOU will build)
✅ Streaming Pipeline (Kafka + Databricks)
Ingest real-time order data using Kafka
Process using Spark Structured Streaming (Databricks)
Store in Delta/Iceberg tables
✅ Batch Pipeline (Airflow + Databricks)
Schedule daily jobs (Airflow)
Process product & customer data
Load into warehouse tables
✅ Data Modeling (IMPORTANT)
Create Dimension Tables
customers (SCD Type 2)
products
Create Fact Table
orders
✅ Historical Tracking
Implement SCD Type 2
Track:
customer updates
product changes
✅ Monitoring Layer
Track:
pipeline failures
data delays
cost usage

this project I want to build

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
    depends_on:
      - zookeeper

  spark-master:
    image: spark:3.5.7-scala2.12-java17-python3-ubuntu
    container_name: spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "8080:8080"
      - "7077:7077"
    depends_on:
      - kafka

  spark-worker:
    image: spark:3.5.7-scala2.12-java17-python3-ubuntu
    container_name: spark-worker
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    environment:
      - SPARK_WORKER_MEMORY=1g
      - SPARK_WORKER_CORES=1
    depends_on:
      - spark-master

```

 
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

# Kafka + Spark Setup using Docker for Real-Time Ride-Sharing Data Platform

This document explains the setup and execution of Apache Kafka and Apache Spark using Docker for building a real-time data engineering pipeline.

---

# 📌 Project Architecture

Producer → Kafka Topic → Spark Structured Streaming → Real-Time Processing

Kafka is used as the real-time event ingestion layer, and Spark is used for stream processing.

---

# 🚀 Step 1 — Create docker-compose.yml

Create a file named:

```bash
docker-compose.yml
```

Add the following content:

```yaml
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
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_INTERNAL://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
    depends_on:
      - zookeeper
  spark-master:
    image: spark:3.5.7-scala2.12-java17-python3-ubuntu
    container_name: spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - C:/Users/HP/ride_streaming_project/ride_streaming_data:/tmp/parquet_data
      - C:/Users/HP/ride_streaming_project/checkpoint:/tmp/checkpoint
    depends_on:
      - kafka
  spark-worker:
    image: spark:3.5.7-scala2.12-java17-python3-ubuntu
    container_name: spark-worker
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    environment:
      - SPARK_WORKER_MEMORY=1g
      - SPARK_WORKER_CORES=1
    volumes:
      - C:/Users/HP/ride_streaming_project/ride_streaming_data:/tmp/parquet_data
      - C:/Users/HP/ride_streaming_project/checkpoint:/tmp/checkpoint
    depends_on:
      - spark-master

```

---

# 🚀 Step 2 — Start Containers

Open terminal/CMD in the project folder and run:

```bash
docker-compose up -d
```

This command starts:

* ZooKeeper container
* Kafka broker container
* Spark master container
* Spark worker container

---

# 🚀 Step 3 — Verify Running Containers

Run:

```bash
docker ps
```

Expected output:

```bash
zookeeper
kafka
spark-master
spark-worker
```

All containers should show status:

```bash
Up
```

---

# 🚀 Step 4 — Create Kafka Topic

Run:

```bash
docker exec -it kafka kafka-topics.sh --create --topic ride_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Explanation:

* Topic Name → `ride_events`
* Partitions → `3`
* Replication Factor → `1`

---

# 🚀 Step 5 — Verify Topic Creation

Run:

```bash
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

Expected output:

```bash
ride_events
```

---

# 🚀 Step 6 — Create Python Kafka Producer

Create a file named:

```bash
producer.py
```

Add the following code:

```python
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample locations
locations = [
    "BTM Layout",
    "Whitefield",
    "Indiranagar",
    "Koramangala",
    "Electronic City",
    "PCMC",
    "PMC",
    "Kharadi",
    "Hadapsar",
    "Kasba Peth"
]

# Continuous Event Generation
while True:

    ride_event = {
        "ride_id": random.randint(1000, 9999),
        "customer_id": random.randint(1, 100),
        "driver_id": random.randint(1000, 2000),

        "pickup_location": random.choice(locations),
        "drop_location": random.choice(locations),

        "fare_amount": random.randint(100, 1000),

        "ride_status": random.choice([
            "BOOKED",
            "STARTED",
            "COMPLETED"
        ]),

        "event_timestamp": datetime.now().isoformat()
    }

    # Send event to Kafka topic
    producer.send("ride_events", ride_event)

    print("Sent Event:", ride_event)

    time.sleep(2)
```

---

# 🚀 Step 7 — Install Python Dependencies

Run:

```bash
pip install kafka-python pyspark
```

---

# 🚀 Step 8 — Create Spark Streaming Job

Create a file named:

```bash
streaming_job.py
```

Add the following code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession.builder \
    .appName("RideStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()

# Set Log Level
spark.sparkContext.setLogLevel("WARN")

# Read Streaming Data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ride_events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka Binary to String
json_df = df.selectExpr("CAST(value AS STRING)")

# Define Schema
schema = StructType([
    StructField("ride_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("pickup_location", StringType(), True),
    StructField("drop_location", StringType(), True),
    StructField("fare_amount", IntegerType(), True),
    StructField("ride_status", StringType(), True),
    StructField("event_timestamp", StringType(), True)
])

# Parse JSON Data
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Write Streaming Data to Parquet
query = parsed_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/tmp/parquet_data") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .option("maxRecordsPerFile", 50) \
    .start()

# Keep Stream Running
query.awaitTermination()
```

---

# 🚀 Step 9 — Copy Spark Job into Spark Container

Run:

```bash
docker cp streaming_job.py spark-master:/opt/spark/work-dir/
```

---

# 🚀 Step 10 — Start Kafka Producer

Open terminal 1 and run:

```bash
python producer.py
```

Expected output:

```bash
Sent: {'ride_id': 1001, 'city': 'Bangalore', 'fare': 250.5}
```

---

# 🚀 Step 11 — Submit Spark Streaming Job

Open terminal 2 and run:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark/work-dir/streaming_job.py
```

---

# 🚀 Step 12 — Verify Streaming Output

Expected output:

```bash
+-----------+------------------+
|city       |avg(fare)         |
+-----------+------------------+
|Bangalore  |245.6             |
|Mumbai     |301.2             |
+-----------+------------------+
```

Spark continuously processes Kafka events in real time.

---

# 🚀 Step 13 — Open Spark UI

Open browser:

```text
http://localhost:8080
```

You can monitor:

* Spark applications
* Executors
* Jobs
* Stages
* Workers

---

# 📌 Key Concepts

| Component | Description |
| --------- | ----------- |
| Producer | Sends streaming events |
| Kafka Topic | Stores streaming events |
| Spark Streaming | Processes events in real time |
| Consumer | Reads events |
| Partition | Enables parallel processing |
| Broker | Kafka server |

---

# 📌 Real-World Usage

This architecture is widely used for:

* Uber/Ola ride tracking
* Real-time payment systems
* Fraud detection
* IoT pipelines
* Log analytics
* Real-time dashboards
* Event-driven microservices

---

# 📌 Tech Stack

| Technology | Purpose |
| ---------- | ------- |
| Docker | Containerization |
| Kafka | Event Streaming |
| Spark | Stream Processing |
| Python | Producer Application |
| ZooKeeper | Kafka Coordination |

---

# 📌 Next Steps

After completing this setup:

1. Connect Spark with Snowflake
2. Build Bronze-Silver-Gold architecture
3. Store streaming data in Delta Lake
4. Create Streamlit dashboards
5. Add Airflow orchestration
6. Implement monitoring and alerts
7. Deploy on Kubernetes

---

# 📌 Useful Commands

## Stop Containers

```bash
docker-compose down
```

## Restart Containers

```bash
docker-compose restart
```

## View Kafka Logs

```bash
docker logs kafka
```

## View Spark Master Logs

```bash
docker logs spark-master
```

## View Spark Worker Logs

```bash
docker logs spark-worker
```

---

# 📌 Learning Outcomes

By completing this project, you will understand:

* Kafka fundamentals
* Spark Structured Streaming
* Real-time event processing
* Docker-based distributed systems
* Producer-consumer architecture
* Streaming analytics pipelines

---

# 📌 Author

**Praveen Devadkar**  
Aspiring Data Engineer | Kafka | Spark | Snowflake | Stream Processing

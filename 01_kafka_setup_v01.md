# Kafka Setup using Docker for Real-Time Ride-Sharing Data Platform

This document explains the setup and execution of Apache Kafka using Docker for building a real-time data engineering pipeline.

---

# 📌 Project Architecture

Producer → Kafka Topic → Consumer

Kafka is used as the real-time event ingestion layer for ride-sharing events.

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
      KAFKA_ADVERTISED_HOST_NAME: localhost
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```

---

# 🚀 Step 2 — Start Kafka and ZooKeeper Containers

Open terminal/CMD in the project folder and run:

```bash
docker-compose up -d
```

This command starts:

* ZooKeeper container
* Kafka broker container

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
```

Both containers should show status:

```bash
Up
```

---

# 🚀 Step 4 — Enter Kafka Container

Run:

```bash
docker exec -it kafka bash
```

---

# 🚀 Step 5 — Verify Kafka Installation

Run:

```bash
find / -name kafka-topics.sh
```

Expected output:

```bash
/opt/kafka/bin/kafka-topics.sh
```

---

# 🚀 Step 6 — Create Kafka Topic

Create a topic named:

```bash
ride_events
```

Run:

```bash
/opt/kafka/bin/kafka-topics.sh --create --topic ride_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Explanation:

* Topic Name → `ride_events`
* Partitions → `3` (parallel processing)
* Replication Factor → `1`

---

# 🚀 Step 7 — Verify Topic Creation

Run:

```bash
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Expected output:

```bash
ride_events
```

---

# 🚀 Step 8 — Start Kafka Producer

Run:

```bash
/opt/kafka/bin/kafka-console-producer.sh --topic ride_events --bootstrap-server localhost:9092
```

Now type sample events:

```bash
hello kafka
ride completed
driver assigned
```

Kafka producer sends events to the topic.

---

# 🚀 Step 9 — Start Kafka Consumer

Open another terminal window.

Enter Kafka container:

```bash
docker exec -it kafka bash
```

Run consumer:

```bash
/opt/kafka/bin/kafka-console-consumer.sh --topic ride_events --bootstrap-server localhost:9092 --from-beginning
```

Expected output:

```bash
hello kafka
ride completed
driver assigned
```

This confirms successful event streaming using Kafka.

---

# 📌 Key Kafka Concepts

| Component | Description                 |
| --------- | --------------------------- |
| Producer  | Sends messages/events       |
| Consumer  | Reads messages/events       |
| Topic     | Stream of events            |
| Partition | Enables parallel processing |
| Broker    | Kafka server                |

---

# 📌 Real-World Usage

Kafka is widely used for:

* Ride booking systems (Uber/Ola)
* Order processing systems (Amazon)
* Real-time analytics
* Streaming pipelines
* Event-driven architectures

---

# 📌 Next Steps

After Kafka setup:

1. Build Python Kafka Producer
2. Generate ride-sharing events
3. Connect Kafka with Databricks
4. Implement Spark Structured Streaming
5. Build Bronze-Silver-Gold architecture

---



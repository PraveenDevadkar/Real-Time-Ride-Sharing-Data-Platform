# Python Kafka Producer & Consumer for Real-Time Ride Events

This document explains how to build a real-time ride event streaming pipeline using Python and Apache Kafka.

---

# 📌 Project Goal

Build a real-time event-driven pipeline where:

```text
Python Producer → Kafka Topic → Python Consumer
```

The producer continuously generates ride-sharing events and sends them to Kafka.
The consumer reads and processes these events in real time.

---

# 📂 Project Structure

```bash
ride-sharing-data-platform/
│
├── kafka/
│   ├── producer.py
│   └── consumer.py
│
├── docker-compose.yml
│
└── README.md
```

---

# 🚀 Step 1 — Install Kafka Python Library

Install Kafka Python client:

```bash
pip install kafka-python
```

Verify installation:

```bash
pip show kafka-python
```

---

# 🚀 Step 2 — Create Kafka Producer

Create file:

```bash
kafka/producer.py
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
    "Electronic City"
]

print("Sending ride events to Kafka topic...")

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

    # Send data to Kafka topic
    producer.send("ride_events", ride_event)

    print("Sent Event:", ride_event)

    time.sleep(2)
```

---

# 🚀 Step 3 — Run Producer

Go to project directory:

```bash
cd ride-sharing-data-platform
```

Run producer:

```bash
python kafka/producer.py
```

Expected output:

```text
Sent Event: {
   "ride_id": 1234,
   "customer_id": 50,
   ...
}
```

Kafka producer now continuously streams ride events.

---

# 🚀 Step 4 — Create Kafka Consumer

Create file:

```bash
kafka/consumer.py
```

Add the following code:

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ride_events',

    bootstrap_servers='localhost:9092',

    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for ride events...")

for message in consumer:
    print(message.value)
```

---

# 🚀 Step 5 — Run Consumer

Open a new terminal.

Run:

```bash
python kafka/consumer.py
```

Expected output:

```text
{
  'ride_id': 1234,
  'customer_id': 22,
  'driver_id': 1450,
  'pickup_location': 'BTM Layout',
  'drop_location': 'Whitefield',
  'fare_amount': 450,
  'ride_status': 'COMPLETED',
  'event_timestamp': '2026-05-09T12:30:45'
}
```

Consumer continuously receives ride events from Kafka topic.

---

# 📌 Key Kafka Concepts

| Component | Description                 |
| --------- | --------------------------- |
| Producer  | Sends events/messages       |
| Consumer  | Reads events/messages       |
| Topic     | Stream of events            |
| Broker    | Kafka server                |
| Partition | Enables parallel processing |

---

# 📌 Why Kafka is Used

Kafka is widely used in:

* Ride booking systems
* Order processing systems
* Real-time analytics
* Event-driven architectures
* Streaming data platforms

Benefits:

* Scalability
* Fault tolerance
* High throughput
* Event replay capability

---

# 📌 Real-World Use Case

This project simulates a ride-sharing platform like Uber/Ola where:

* Customers book rides
* Drivers accept rides
* Ride events are streamed in real time
* Multiple systems can consume the same events

---

# 📌 Next Steps

After Kafka producer/consumer setup:

1. Integrate Kafka with Databricks
2. Implement Spark Structured Streaming
3. Build Bronze-Silver-Gold architecture
4. Store streaming data in Delta Lake
5. Create analytical fact and dimension tables

---

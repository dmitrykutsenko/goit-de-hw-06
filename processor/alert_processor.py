import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

from config import (
    KAFKA_BROKER,
    BUILDING_TOPIC,
    TEMP_TOPIC,
    HUMIDITY_TOPIC
)

consumer = KafkaConsumer(
    BUILDING_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m:
        json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="alert_processor_group_v2"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v:
        json.dumps(v).encode("utf-8")
)

print("Processor started")
print(f"Listening topic: {BUILDING_TOPIC}")

for msg in consumer:

    data = msg.value

    print(f"\nRECEIVED: {data}")

    sensor_id = data["sensor_id"]
    temperature = data["temperature"]
    humidity = data["humidity"]
    timestamp = data["timestamp"]

    if temperature > 40:

        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": temperature,
            "message": (
                f"Temperature exceeded: {temperature} C"
            )
        }

        print(f"TEMP ALERT GENERATED: {alert}")

        producer.send(
            TEMP_TOPIC,
            value=alert
        )

        producer.flush()

        print(f"SENT TO TOPIC: {TEMP_TOPIC}")

    if humidity > 80 or humidity < 20:

        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "humidity": humidity,
            "message": (
                f"Humidity out of range: {humidity}%"
            )
        }

        print(f"HUMIDITY ALERT GENERATED: {alert}")

        producer.send(
            HUMIDITY_TOPIC,
            value=alert
        )

        producer.flush()

        print(f"SENT TO TOPIC: {HUMIDITY_TOPIC}")
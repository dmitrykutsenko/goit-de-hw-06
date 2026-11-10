from kafka.admin import KafkaAdminClient, NewTopic

from config import (
    KAFKA_BROKER,
    SENSOR_TOPIC,
    ALERT_TOPIC,
)


admin = KafkaAdminClient(
    bootstrap_servers=KAFKA_BROKER,
    client_id="topic_creator"
)

topics = [
    NewTopic(
        name=SENSOR_TOPIC,
        num_partitions=1,
        replication_factor=1
    ),
    NewTopic(
        name=ALERT_TOPIC,
        num_partitions=1,
        replication_factor=1
    )
]

existing = admin.list_topics()

topics_to_create = [
    topic for topic in topics
    if topic.name not in existing
]

if topics_to_create:
    admin.create_topics(new_topics=topics_to_create)
    print("Topics created")
else:
    print("Topics already exist")

admin.close()

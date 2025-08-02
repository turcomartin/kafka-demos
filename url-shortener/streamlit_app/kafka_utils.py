import json

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import UnknownTopicOrPartitionError  # type: ignore

HOST = "localhost"
HOST = "host.internal.docker"
HOST = "kafka"
BOOTSTRAP_SERVER = "kafka:29092"


def fetch_latest_records(broker, topic, timeout_ms=1000, MAX_MESSAGES=5):
    """
    Fetches the latest records from a Kafka topic.

    Args:
        broker (str): The Kafka broker address (e.g., "localhost:9092").
        topic (str): The Kafka topic name.
        timeout_ms (int): Timeout in milliseconds for consumer operations.

    Returns:
        list: A list of messages from the latest records.
    """

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset="earliest",  # Or 'latest' to start from the end if offset is already committed
        enable_auto_commit=False,
        group_id=None,  # Set to None for a simple consumer
        consumer_timeout_ms=timeout_ms,
        api_version=(4, 0, 0),
    )

    # Get Topic offset
    partitions = [
        TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)
    ]

    # Get current end offsets
    end_offsets = consumer.end_offsets(partitions)
    messages = []
    for key, value in end_offsets.items():
        print(f"Partition: {key.partition}, End Offset: {value}")

        value = value - 1  # Adjust to get the last message
        offsets_to_fetch = [value - i for i in range(MAX_MESSAGES) if value - i >= 0][
            ::-1
        ]
        for offset in offsets_to_fetch:
            tp = TopicPartition(topic, key.partition)
            # consumer.assign([tp])
            consumer.seek(tp, offset)
            msg = consumer.poll(
                timeout_ms=timeout_ms, max_records=1
            )  # max_records default is 500 😅, too much
            messages.append(msg)
    consumer.close()
    return messages


def purge_topic(topic, offset=None):
    try:
        admin = KafkaAdminClient(
            # bootstrap_servers=f"{HOST}:9093",
            bootstrap_servers=BOOTSTRAP_SERVER,
            api_version=(4, 0, 0),  # Specify the Kafka version
        )

        if offset is None:
            # If no offset is provided, delete all records in the topic
            admin.delete_topics([topic])
        else:
            # If an offset is provided, delete records up to that offset
            admin.delete_records({TopicPartition(topic, 0): offset})
    except UnknownTopicOrPartitionError:
        print(f"Topic '{topic}' does not exist.")


def parse_kafka_message(message):
    """Parse the Kafka message to extract the original and short URL."""
    return json.loads(list(message.values())[0][0].value)

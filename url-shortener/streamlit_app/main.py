import json
import logging

import redis
import streamlit as st
from kafka import KafkaConsumer, KafkaProducer  # type: ignore
from kafka_utils import fetch_latest_records, parse_kafka_message, purge_topic

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()


def refresh_app():
    st.rerun()


HOST = "localhost"
HOST = "host.internal.docker"
BROKER = "kafka"

# BOOTSTRAP_SERVER = f"{BROKER}:9092"
# BOOTSTRAP_SERVER = f"{BROKER}:9092"
BOOTSTRAP_SERVER = "kafka:29092"
BOOTSTRAP_CONTROLLER = "broker:29093"
NEW_URL_TOPIC_NAME = "new_url_shortener"
DELETE_URL_TOPIC_NAME = "delete_url_shortener"

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(4, 0, 0),  # Specify the Kafka version
)

print(f"Connecting to Kafka at {BOOTSTRAP_SERVER}...")


# Connect to Redis
print(f"Connecting to Redis at {HOST}:6379...")
# r = redis.Redis(host=HOST, port=6379, db=0)
r = redis.Redis(host="redis", port=6379, db=0)


def post_to_kafka(topic_name, original_url, short_url):
    """Post the original and short URL to Kafka."""
    print("Posting to Kafka...")
    producer.send(topic_name, {"original_url": original_url, "short_url": short_url})
    producer.flush()


# post_to_kafka(NEW_URL_TOPIC_NAME, "mytestlongwebsite.com", "mytestshortweb.com")


def consume_from_kafka(topic_name):
    """Consume messages from Kafka."""
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        max_poll_records=2,
        api_version=(4, 0, 0),  # Specify the Kafka version
    )
    return consumer


def post_to_redis(redis_key, redis_value):
    """Post the long and short URL to Redis."""
    r.set(redis_key, redis_value)


def delete_from_redis(long_url):
    """Delete the long URL from Redis."""
    r.delete(long_url)


st.write("# Tracking Redis events with Kafka 😉")
st.write("")
st.set_page_config(layout="wide")
col1, col2 = st.columns(2)
with col1:
    redis_key = st.text_input("Key", "my key")
    redis_value = st.text_input("Value", "my value")

    # Get all key-value pairs from Redis
    all_urls = r.keys("*")
    url_dict = {key.decode("utf-8"): r.get(key).decode("utf-8") for key in all_urls}

    # Submit button
    if st.button("Submit"):
        # Store the URL in Redis
        post_to_redis(redis_key, redis_value)
        post_to_kafka(NEW_URL_TOPIC_NAME, redis_value, redis_key)
        refresh_app()

    # Display all key value pairs in Redis as a table
    # Add table headers
    st.write("### All URLs in Redis:")
    header_cols = st.columns([4, 4, 1])
    header_cols[0].markdown("**Keys**")
    header_cols[1].markdown("**Values**")
    header_cols[2].markdown("**Delete**")

    for short, long in url_dict.items():
        cols = st.columns([4, 4, 1])
        cols[0].write(short)
        cols[1].write(long)
        if cols[2].button(
            "",
            key=f"delete_{long}",
            icon="❌",
            type="tertiary",
            use_container_width=True,
        ):
            delete_from_redis(short)
            post_to_kafka(DELETE_URL_TOPIC_NAME, long, short)
            refresh_app()


with col2:
    st.write("## Kafka Events")
    full = st.toggle("Show Full Kafka Events", value=False, key="show_kafka_events")
    if st.button("Clean Topics"):
        # Clean the topic by deleting all messages
        purge_topic(NEW_URL_TOPIC_NAME)
        purge_topic(DELETE_URL_TOPIC_NAME)
        refresh_app()

    col21, col23 = st.columns(2)
    with col21:
        st.write("#### New elements Events:")
        # Display the events in Kafka
        # Get all messages from the consumer
        messages = fetch_latest_records(BOOTSTRAP_SERVER, NEW_URL_TOPIC_NAME)
        messages = messages[::-1]  # Reverse the order to show the latest first
        # Display the last 5 messages
        if full:
            for message in messages[-5:]:
                if message:
                    st.write(f"• {message}")
        else:
            for message in messages[-5:]:
                if message:
                    st.write(f"• {parse_kafka_message(message)}")
    with col23:
        st.write("#### Delete elements Events:")
        i = 0
        messages = fetch_latest_records(BOOTSTRAP_SERVER, DELETE_URL_TOPIC_NAME)
        messages = messages[::-1]  # Reverse the order to show the latest first
        # Display the last 5 messages
        if full:
            for message in messages[-5:]:
                if message:
                    st.write(f"• {message}")
        else:
            for message in messages[-5:]:
                if message:
                    st.write(f"• {parse_kafka_message(message)}")

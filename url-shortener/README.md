# Redis event tracking with Kakfa

This is a toy project to see how we can combine kafka, redis and Streamlit. The simplistic approach ensures you can get familiar with the tools, experiment and get ready to explore these technologies more in-depth

## What are we building?

We are building a Streamlit interface to insert key-value pairs of data into a Redis database. Each entry will also send a message to a Kakfa broker.
This will allow any other service listening to these messages take action if needed.

## Our components

- Streamlit
- Docker
- Kafka
- Redis
- FastAPI (Optional)

## Kafka

[Kafka](https://kafka.apache.org/documentation/) is a message broker that is based on the concept of topic. Think of the topic a way to categorize messages so you only pay attention to what you are interested in.
It receives messages from producers, stores them, and serves them to consumers upon request

## Streamlit

[Streamlit](https://streamlit.io) is an open-source Python framework for engineers to deliver interactive data apps – in only a few lines of code.

Stramlit will be our interface with Redis and will allow us to see the messages published to Kafka

## Docker Containers

[Docker](https://www.docker.com/) packages software into standardized units called containers that have everything the software needs to run including libraries, system tools, code, and runtime.

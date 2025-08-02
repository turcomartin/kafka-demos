# URL Shortener

In this tutorial we are going to build a completely serverless URL Shortener with real time monitoring and observability.

![Architechture of the URL shortener](url-shortener.png)

## Our shopping cart 🛒

- We got a **Redis DB** to store the short-urls and original urls.
- A **Kafka** server that will store incoming click events
- A **Cloudflare** worker that will host our URL Shortener
- A **Grafana** cloud service we can connect to Kafka and create some nice visualizations.

## Redis

We head up to [upstash.com](upstash.com) and create a new account.

Head to the Redis section and boot up a new Redis instance.

Let's name **url-shortener-redis**.

## Kafka

# Solving the Dual Write Problem with Debezium

Static GitHub Pages slide deck plus a runnable local lab for explaining reliable asynchronous event delivery with MongoDB, Confluent Kafka, Go services, the outbox pattern, and Debezium CDC.

## Repository Layout

```text
presentation/
  Static GitHub Pages slide deck and speaker notes.

demo/
  Runnable MongoDB, Confluent Kafka, Debezium, Orders API, and Fulfillment consumer lab.
```

## Preview the GitHub Page

Open `presentation/index.html` directly in a browser, or serve the repository root with any static file server:

```sh
python3 -m http.server 8000
```

Then open either URL:

```text
http://localhost:8000
http://localhost:8000/presentation/
```

The root `index.html` redirects to `presentation/` so GitHub Pages can still publish from the repository root.

## Navigation

- Arrow right, Page Down, or Space: next slide.
- Arrow left or Page Up: previous slide.
- Home: first slide.
- End: last slide.
- Hash links open specific slides, for example `#dual-write`, `#outbox-pattern`, `#demo`, and `#run-lab`.

## Speaker Notes

Use [presentation/SPEAKER_NOTES.md](presentation/SPEAKER_NOTES.md) as the Markdown source for the 60-minute talk track. Open [presentation/speaker-notes.html](presentation/speaker-notes.html) for the browser-friendly notes page with timing, slide-by-slide notes, live demo commands, expected outputs, and fallback flow.

## Run the Lab

Prerequisite: Docker Desktop or another Docker engine with Docker Compose.

Host ports are intentionally offset to avoid common local service conflicts:

```text
orders-api: http://localhost:18080
debezium-connect: http://localhost:18083
kafka: localhost:19092
mongodb: localhost:27018
```

Runtime versions are pinned to current explicit tags for reproducible lab runs:

```text
Go builder: golang:1.26.4-alpine3.23
Go runtime: alpine:3.23.4
MongoDB: mongo:8.3.2
Confluent Kafka: confluentinc/cp-kafka:8.2.1
Debezium Connect: quay.io/debezium/connect:3.5.2
curl helper: curlimages/curl:8.20.0
```

Service boundaries:

```text
orders-api
  owns checkout.orders and checkout.outbox_events
  exposes POST /checkouts/{checkoutID}/complete
  writes business state and outbox event in one MongoDB transaction

fulfillment-consumer
  owns fulfillment.processed_events and fulfillment.fulfillments
  consumes marketplace.orders.events.v1 with consumer group fulfillment-service
  persists event ids before applying fulfillment side effects
```

Start MongoDB, Confluent Kafka, Debezium Connect, the connector registration job, the Orders API microservice, and the Fulfillment consumer microservice:

```sh
cd demo
docker compose up --build -d
```

Run the remaining lab commands from the `demo/` directory.

Check the Debezium connector:

```sh
sh scripts/watch-connect.sh
```

Produce an order completion event through the Go API:

```sh
sh scripts/create-checkout.sh checkout-1001
```

The Orders API writes `checkout.orders` and `checkout.outbox_events` in the same MongoDB transaction. Debezium captures the committed `outbox_events` document and publishes to:

```text
marketplace.orders.events.v1
```

## Monitor Messages And Database State

Watch Kafka messages from the beginning:

```sh
sh scripts/watch-kafka.sh
```

Inspect MongoDB state for both microservices:

```sh
sh scripts/watch-mongo.sh
```

Watch the Fulfillment consumer process. It persists processed event IDs in `fulfillment.processed_events` and fulfillment side effects in `fulfillment.fulfillments`:

```sh
docker compose logs -f fulfillment-consumer
```

List Kafka topics:

```sh
docker compose exec broker kafka-topics --bootstrap-server broker:29092 --list
```

Inspect connector tasks directly:

```sh
curl -sS http://localhost:18083/connectors/mongodb-outbox/status
```

Stop and remove lab data:

```sh
docker compose down -v
```

## GitHub Pages

The presentation page has no package manager, build step, runtime backend, or external CDN dependency. The runnable lab is isolated under `demo/` and is not required for GitHub Pages.

To publish from GitHub:

1. Open the repository settings on GitHub.
2. Go to **Pages**.
3. Set source to **Deploy from a branch**.
4. Select branch `main` and folder `/ (root)`.
5. Save.

GitHub Pages will serve the root `index.html`, which redirects to `presentation/`.

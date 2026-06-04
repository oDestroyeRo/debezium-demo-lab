# Speaker Notes: Solving the Dual Write Problem with Debezium

Target length: 60 minutes, including live demo.

Audience: backend engineers who know service APIs, databases, and Kafka basics.

Primary goal: show why direct database plus Kafka writes are unsafe, then prove how MongoDB outbox plus Debezium makes event publication durable and observable.

## Timing Plan

| Time | Section | Slides | Goal |
| --- | --- | --- | --- |
| 00:00-05:00 | Setup and goal | Intro, Agenda | Frame the problem and expected takeaways. |
| 05:00-12:00 | Communication modes | Sync vs Async | Compare request coupling with event-based workflows. |
| 12:00-24:00 | Failure model | Dual Write, Inconsistent State | Make the dual write failure concrete. |
| 24:00-34:00 | Pattern | Outbox Pattern, Outbox Flow | Show the single commit boundary. |
| 34:00-44:00 | Debezium | Debezium, CDC Pipeline | Explain CDC relay behavior and limits. |
| 44:00-56:00 | Live demo | Demo, Demo Event, Consumer, Run Lab | Run the lab and inspect API, MongoDB, Kafka, Debezium, consumer. |
| 56:00-60:00 | Wrap | Lessons Learned | Summarize decisions, risks, and operational checklist. |

Keep 3-5 minutes free for Docker startup delays or questions.

## Preflight Before The Talk

Run this before presenting:

```sh
cd demo
docker compose down -v
docker compose build
docker compose config
go test ./...
```

Check that these host ports are free or be ready to explain the offset ports:

```text
orders-service: http://localhost:18080
debezium-connect: http://localhost:18083
kafka: localhost:19092
mongodb: localhost:27018
```

Open three terminal panes:

```sh
# pane 1: stack and logs
cd demo
docker compose up --build -d
docker compose logs -f fulfillment-service
```

```sh
# pane 2: commands during demo
cd demo
sh scripts/watch-connect.sh
sh scripts/create-checkout.sh checkout-demo-001
sh scripts/watch-mongo.sh
```

```sh
# pane 3: Kafka inspection
cd demo
sh scripts/watch-kafka.sh
```

If network or image pulls are slow, use the storyboard slides and show the already committed files:

```sh
demo/docker-compose.yml
demo/deploy/debezium/mongodb-outbox-connector.json
demo/cmd/orders-service/main.go
demo/cmd/fulfillment-service/main.go
demo/internal/orders/
demo/internal/fulfillment/
```

## Opening Script

Say:

Today we are solving one specific backend reliability problem: a service commits state to a database and also needs to notify other services through Kafka. The dangerous version is a dual write: write database, then publish Kafka, or publish Kafka, then write database. Those two systems do not share one transaction, so failure timing can split reality.

The target stack today is Go services, MongoDB, Confluent Kafka, and Debezium. The key idea is not "Kafka makes everything reliable" and not "Debezium gives business exactly-once." The key idea is: put the event intent in MongoDB with the business state, then let Debezium publish committed changes to Kafka.

## Slide Notes

### 1. Intro: Solving the Dual Write Problem with Debezium

Time: 3 minutes.

Say:

This deck has two parts. First, the reasoning: why dual writes fail and how the outbox pattern changes the consistency boundary. Second, the lab: `orders-service` writes an order and an outbox event into MongoDB, Debezium captures the outbox document, Kafka receives the event, and `fulfillment-service` processes it.

Point at the flow:

```text
orders-service -> MongoDB -> Debezium -> Confluent Kafka -> fulfillment-service
```

Emphasize:

- MongoDB is the source of truth for the business state and event intent.
- Debezium is the relay from committed database changes to Kafka.
- Kafka is the fan-out and replay surface.
- `fulfillment-service` still needs idempotency.

Transition:

Before the fix makes sense, we need to agree on the communication tradeoff.

### 2. Agenda

Time: 2 minutes.

Say:

We will move from communication style, to the dual write failure, to the outbox pattern, then into Debezium and the runnable demo. The point is not to memorize Debezium configuration. The point is to identify where the transaction boundary should be.

Keep this fast. Do not explain every section yet.

Transition:

Start with synchronous versus asynchronous communication.

### 3. Synchronous vs Async

Time: 7 minutes.

Say:

Synchronous communication is easy to reason about at first: service A calls service B and waits. It is good when the caller needs an immediate decision. But it couples latency and availability. If the callee is slow, the caller is slow. If the callee times out, the caller may not know whether the callee did the work.

Async communication decouples the request path. Service A commits its own state and emits a fact. Service B reacts later. That helps scalability and resilience, but correctness does not disappear. It moves into delivery semantics, ordering, retries, deduplication, and monitoring.

Example:

```text
Synchronous: checkout-api calls fulfillment-api directly.
Asynchronous: checkout-api publishes order.completed, fulfillment consumes it.
```

Important contrast:

- Sync failure: timeout creates unknown remote state.
- Async failure: local commit succeeds but event delivery must be reliable.

Transition:

Async is useful, but the naive implementation creates a different failure: dual write.

### 4. Dual Write Problem

Time: 6 minutes.

Say:

The dual write problem happens when one request tries to commit two independent systems. In our stack, that means the Go service writes MongoDB and publishes Kafka in separate operations. MongoDB and Kafka do not share a transaction. There is no atomic commit across both.

Use the code on the slide:

```go
orderID := orders.Insert(ctx, cmd)
return producer.Publish(ctx, Event{Type: "order.completed"})
```

Ask:

What happens if the process crashes between those two lines?

Answer:

MongoDB says the order is completed. Kafka has no event. Consumers never learn about the completed order unless we run manual repair.

Then ask:

What happens if Kafka publish succeeds but the service times out before it sees the acknowledgement?

Answer:

Retry may publish a duplicate. Now the consumer must know how to dedupe.

Transition:

Let us categorize the inconsistent states.

### 5. Why Database Plus Kafka Can Become Inconsistent

Time: 6 minutes.

Say:

There are four common failure windows.

First, database commit succeeds and publish fails. The database is ahead of Kafka.

Second, publish succeeds but the acknowledgement is lost. The service retries and duplicates the event.

Third, publish succeeds but the database operation rolls back. Kafka announced a state that never existed.

Fourth, manual recovery republishes incorrectly. This can skip headers, use the wrong key, reorder events, or emit a payload shape consumers do not expect.

Key phrase:

The bug is not just "Kafka failed." The bug is "two separate commits represent one business fact."

Transition:

The outbox pattern fixes the commit boundary.

### 6. Outbox Pattern

Time: 5 minutes.

Say:

The outbox pattern says: do not publish directly to Kafka inside the request transaction. Instead, write the business state and an outbox event record in the same database transaction.

In this lab:

```text
checkout.orders
checkout.outbox_events
```

`orders-service` writes both:

```text
orders: status = COMPLETED
outbox_events: type = order.completed
outbox_events: aggregatetype = orders
outbox_events: aggregateid = order id
outbox_events: payload.event_id = event id
```

Now if the request commits, both the order and event intent exist. If the request rolls back, neither exists.

Important:

The outbox does not publish by itself. It stores durable intent. A relay still has to move that intent to Kafka.

Transition:

Now we need a relay. That is where Debezium enters.

### 7. Outbox Flow

Time: 5 minutes.

Say:

The request flow changes:

```text
API request -> orders-service -> MongoDB transaction -> CDC relay -> Kafka topic
```

`orders-service` owns validation and transaction intent. MongoDB owns the committed truth. Debezium owns observing committed outbox changes and publishing them to Kafka.

This is the strongest sentence on the slide:

Debezium does not replace the outbox write. Debezium observes the committed outbox document.

Why that matters:

If someone says "we use Debezium, so we do not need outbox," ask what event contract consumers will receive. Raw database change events are table or collection changes. Outbox events are business facts.

Transition:

Now look at what Debezium actually does.

### 8. How Debezium Solves It

Time: 5 minutes.

Say:

Debezium is a CDC engine. CDC means change data capture. It reads committed database changes and streams them into Kafka Connect. For MongoDB, it watches change streams from the replica set.

In this lab, Debezium is configured to watch only:

```text
checkout.outbox_events
```

Then the MongoDB outbox event router transforms the outbox document into an application-level Kafka message.

Point out the config shape:

```json
"connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
"mongodb.connection.string": "mongodb://mongo:27017/?replicaSet=rs0",
"topic.prefix": "demo",
"database.include.list": "checkout",
"collection.include.list": "checkout.outbox_events",
"capture.mode": "change_streams_update_full",
"transforms.outbox.type": "io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter",
"transforms.outbox.route.topic.replacement": "${routedByValue}.events.v1"
```

Emphasize:

- Connector offsets let Debezium resume after restart.
- Kafka Connect manages the connector process.
- Kafka holds the messages for consumers.
- Delivery is still at-least-once from the application point of view.

Transition:

The pipeline becomes easier to observe.

### 9. CDC Pipeline

Time: 5 minutes.

Say:

The pipeline has five responsibilities.

`orders-service` writes one transaction. MongoDB commits one source of truth. Debezium captures the outbox change. Kafka receives the event. `fulfillment-service` processes idempotently.

Explain the boundary:

Debezium improves the reliability between MongoDB and Kafka because it reads committed changes and tracks offsets. But the consumer side still sees a distributed system. Consumers must be safe during retry, rebalance, replay, and duplicate delivery.

Practical checks:

```sh
cd demo
sh scripts/watch-connect.sh
sh scripts/watch-kafka.sh
sh scripts/watch-mongo.sh
docker compose logs -f fulfillment-service
```

Transition:

Now we will run the exact flow.

## Demo Notes

Target time: 12 minutes.

Run commands in this section from the `demo/` directory.

### 10. Demo Storyboard

Time: 2 minutes before commands.

Say:

The demo follows one checkout:

1. Send `POST /checkouts/{id}/complete`.
2. `orders-service` inserts one `orders` document and one `outbox_events` document in the same MongoDB transaction.
3. Debezium captures the committed outbox event.
4. Kafka receives the message on `orders.events.v1`.
5. `fulfillment-service` records the processed event id, then writes a fulfillment record.

### Start The Lab

Run:

```sh
cd demo
docker compose up --build -d
```

Say while it starts:

This starts MongoDB as a single-node replica set because MongoDB change streams need replica-set behavior. It starts one Confluent Kafka broker in KRaft mode, creates the `orders.events.v1` topic, starts Debezium Connect, runs a one-shot connector registration container, starts `orders-service`, and then starts `fulfillment-service`.

Check:

```sh
docker compose ps
```

Expected:

```text
broker: healthy
topic-init: exited 0
mongo: up
connect: up
orders-service: up
fulfillment-service: up
```

If Connect is still starting, wait and say:

Kafka and Connect are infrastructure pieces. They often take longer than the app. The important thing is that the connector reaches `RUNNING`.

### Check Debezium

Run:

```sh
sh scripts/watch-connect.sh
```

Expected shape:

```json
{
  "name": "mongodb-outbox",
  "connector": { "state": "RUNNING" },
  "tasks": [{ "state": "RUNNING" }]
}
```

Say:

This proves Debezium Connect accepted the MongoDB outbox connector and its task is running.

### Produce One Checkout

Run:

```sh
sh scripts/create-checkout.sh checkout-demo-001
```

Expected shape:

```json
{
  "checkout_id": "checkout-demo-001",
  "order_id": "...",
  "outbox_event_id": "...",
  "topic": "orders.events.v1"
}
```

Say:

The `orders-service` response gives us both IDs. The order id is the aggregate id. The outbox event id is the stable event id used by `fulfillment-service` for dedupe.

### Inspect MongoDB

Run:

```sh
sh scripts/watch-mongo.sh
```

Say:

Look for Orders state first, then Fulfillment state after `fulfillment-service` processes the event.

Business state:

```text
orders.status = COMPLETED
```

Event intent:

```text
outbox_events.type = order.completed
outbox_events.aggregateid = order id
outbox_events.payload.event_id = outbox event id
outbox_events.payload.event_version = 1
```

Fulfillment state:

```text
processed_events._id = outbox event id
fulfillments.order_id = order id
fulfillments.status = STARTED
```

Important:

The Orders records exist because they committed together. Fulfillment records exist only after Kafka delivery and `fulfillment-service` processing.

### Inspect Kafka

Run:

```sh
sh scripts/watch-kafka.sh
```

Expected:

```text
headers include id:<outbox_event_id>, event_type:order.completed
key is the order id
value contains event_id, event_version, checkout_id, order_id, shop_id, total_amount
```

Say:

Debezium converted the MongoDB outbox document into a Kafka event. The key supports per-order ordering. The event id supports dedupe. The topic name is business-facing, not a raw collection name.

Stop the consumer after one or two messages with `Ctrl-C`.

### Inspect Fulfillment Service

Run:

```sh
docker compose logs -f fulfillment-service
```

Expected:

```text
fulfillment started event_id=... order_id=... topic=orders.events.v1 offset=...
```

Say:

This is now a real microservice boundary. `orders-service` owns `checkout.orders` and `checkout.outbox_events`. `fulfillment-service` owns `fulfillment.processed_events` and `fulfillment.fulfillments`. It inserts the processed event id first, then upserts the fulfillment side effect in the same MongoDB transaction.

### Produce A Second Event

Run in another pane:

```sh
sh scripts/create-checkout.sh checkout-demo-002
```

Say:

Watch the `fulfillment-service` log. `orders-service` did not publish to Kafka. It only wrote MongoDB. The event still appears because Debezium watches the outbox collection.

### Optional Failure Explanation

Do not break the live stack unless there is enough time.

Say:

If the API crashes after MongoDB commit, the outbox event is still stored. Debezium can publish after the API process is gone. If Debezium or Kafka is temporarily down, MongoDB still keeps the outbox event, and Debezium resumes from offsets when it recovers.

What this does not solve:

- It does not make consumer side effects exactly-once.
- It does not remove the need for idempotency.
- It does not replace schema/version discipline.
- It does not remove monitoring needs.

## Slide Notes After Demo

### 11. Demo Event Shape

Time: 3 minutes.

Say:

The event shape is intentionally boring. Boring is good for event contracts.

Call out:

- `event_id`: unique event identity for dedupe.
- `event_type`: business fact, here `order.completed`.
- `aggregate_id`: the order id used as Kafka key.
- `event_version`: version boundary for future evolution.
- `occurred_at`: when the source event happened.
- `payload`: domain data needed by consumers.

Event body should not contain secrets or raw tokens. Kafka retention and consumer access make sensitive payloads expensive to clean up.

### 12. Consumer Responsibility

Time: 3 minutes.

Say:

Debezium and Kafka can deliver at least once. They cannot know whether your business side effect was safe to repeat. The consumer owns that.

The production pattern is usually:

```text
begin local transaction
insert processed_event(event_id)
if duplicate, exit
apply side effect
commit local transaction
```

In this demo `fulfillment-service` uses MongoDB collections for the inbox pattern: `processed_events` for idempotency and `fulfillments` for the side effect. A duplicate event id becomes a no-op, while a new event upserts one fulfillment per `order_id`.

### 13. Run Lab

Time: 2 minutes if already demoed, 8 minutes if not.

Say:

This slide is the operator runbook. If someone wants to repeat the demo, these are the commands.

Point out:

- `watch-connect.sh` monitors Debezium connector state.
- `watch-kafka.sh` monitors messages.
- `watch-mongo.sh` monitors Orders and Fulfillment database state.
- `docker compose logs -f fulfillment-service` monitors Fulfillment service behavior.

### 14. Lessons Learned

Time: 4 minutes.

Say:

The lesson is not "always use Debezium." The lesson is to make the consistency boundary explicit.

First, use one commit boundary. For a MongoDB-owned aggregate, write domain state and event intent together.

Second, keep events boring. Stable id, clear type, aggregate key, version, and timestamp.

Third, assume at-least-once delivery. Design consumers for replay and duplicates.

Fourth, monitor the relay. Operators need connector status, lag, Kafka topic visibility, DLQ or quarantine policy, and schema drift checks.

Close:

The outbox pattern gives us durable intent. Debezium gives us a reliable relay from committed changes to Kafka. Kafka gives us fan-out and replay. Consumers still own idempotent business behavior.

## Q&A Prompts

Use these if discussion is quiet:

- Where would you put the outbox write in our current service layers?
- Which aggregate id should become the Kafka key?
- Which consumers need ordering?
- What side effects are not safe to repeat?
- What should happen to poison messages?
- How long should Kafka retain these events?
- Which fields are safe to put in the event body?
- What monitoring would prove the pipeline is healthy?

## Common Questions And Answers

### Does Debezium remove the need for Kafka producers in services?

For outbox-backed domain events, yes, the request handler should not directly publish to Kafka. It writes outbox records. Debezium publishes those records. Services may still use direct producers for non-transactional signals, but not for facts that must match a database commit.

### Why not publish directly to Kafka first, then write MongoDB?

Then Kafka can announce a fact that MongoDB never commits. Consumers would act on false state.

### Why not write MongoDB first, then publish Kafka with retry?

Retry helps availability, but it cannot close the crash window between database commit and publish. It also creates duplicate risk when acknowledgements are ambiguous.

### Is this exactly-once?

No. It gives atomic persistence of state and event intent in MongoDB, then reliable CDC publication. Consumers must still handle at-least-once delivery.

### Why an outbox collection instead of raw collection CDC?

Raw collection CDC describes database mutations. An outbox event describes a business fact. Consumers should depend on business contracts, not internal document shape.

### What should be monitored?

Monitor Debezium connector status, connector task errors, source lag, Kafka topic production, consumer lag, DLQ or quarantine volume, and MongoDB outbox growth.

## Demo Cleanup

Run after the session:

```sh
cd demo
docker compose down -v
```

Say if asked:

`-v` removes demo data so the next run starts from a clean MongoDB and Kafka state.

## Fallback If The Live Demo Fails

If Docker fails:

1. Show `demo/docker-compose.yml`.
2. Show `demo/deploy/debezium/mongodb-outbox-connector.json`.
3. Show the order transaction in `demo/internal/orders/repository/mongo/repository.go`.
4. Show the durable consumer idempotency path in `demo/internal/fulfillment/repository/mongo/repository.go`.
5. Explain expected outputs from these commands:

```sh
cd demo
sh scripts/watch-connect.sh
sh scripts/create-checkout.sh checkout-demo-001
sh scripts/watch-mongo.sh
sh scripts/watch-kafka.sh
docker compose logs -f fulfillment-service
```

Close with:

The implementation details can vary, but the invariant must not: one business fact should have one durable commit boundary before it enters Kafka.

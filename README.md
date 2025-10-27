# Order Processing Service

Short description

This service receives `order-created` events, waits for payment, and forwards orders to the Food Service by publishing to `order-processed` when both order data and payment are available.

Key info
- Java: 21
- Spring Boot: 3.5.7
- Kafka topics used: `order-created`, `order-processed`, `payment-completed`
- No HTTP controllers (event-driven)

Prerequisites
- Java 21
- Maven / `mvnw.cmd`
- Kafka cluster

Configuration
- `src/main/resources/env.properties` contains bootstrap and group id.

Build and run

```bash
cd order-processing-service-main
mvnw.cmd -DskipTests package
java -jar target/order-processing-service-0.0.1-SNAPSHOT.jar
```

Kafka

- Consumes: `order-created`, `payment-completed`
- Publishes: `order-processed`

Troubleshooting

- Ensure Kafka is reachable and topics exist. Check logs for order processing events.
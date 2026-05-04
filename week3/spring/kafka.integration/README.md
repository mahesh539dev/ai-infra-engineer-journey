# Spring Boot: Kafka Producer + Reactive Search Gateway

A Spring Boot 4 application with two responsibilities: publishing AI learning events to Kafka via Spring Cloud Stream, and exposing a reactive REST endpoint that proxies semantic search queries to a downstream search service.

---

## What This Does

- Publishes `KafkaEvent` messages (message, topic, subtopic) to the `learning-ai` Kafka topic
- Exposes `GET /retrieve?topic=<value>` — a non-blocking REST endpoint that calls a downstream vector search service and returns ranked results
- Handles all error scenarios (validation, downstream failures, service unavailability) with structured JSON error responses

---

## Architecture

```
HTTP Client
    │
    ▼
GET /retrieve?topic=X
    │
    ▼
RetrieveInfo (Handler)          ← validates query param, throws ValidationException if blank
    │
    ▼
RetrieveService                 ← WebClient call to downstream search service
    │   GET {search.api.base-url}/retrieve-by-topic?topic=X
    │
    ▼
Response { topicResults: Map<String, ResultEntry> }
    │   ResultEntry { text, metadata, similarity }
    ▼
JSON response to client

─────────────────────────────────

Spring Cloud Stream
    │
    ├── binding: ai-producer → topic: learning-ai
    ├── serializer: JsonSerializer
    └── broker: localhost:9092
```

---

## Setup

### Prerequisites

- Java 17+
- Maven 3.8+
- Kafka running on `localhost:9092` (see `docker-compose.yml` in the `week3/` root)
- A downstream search service running (configured via `search.api.base-url`)

### Configuration (`src/main/resources/application.yaml`)

```yaml
spring:
  cloud:
    stream:
      bindings:
        ai-producer:
          destination: learning-ai   # Kafka topic name
      kafka:
        binder:
          brokers: localhost:9092

search:
  api:
    base-url: localhost:8000         # downstream search service

server:
  port: 8090
```

### Run

```bash
./mvnw spring-boot:run
```

---

## API Reference

### `GET /retrieve`

Searches the vector index for results matching the given topic.

**Query Parameters**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `topic` | Yes | Topic string to search — must not be blank |

**Example Request**

```
GET http://localhost:8090/retrieve?topic=machine-learning
```

**Example Response**

```json
{
  "topic_results": {
    "result-1": {
      "text": "Introduction to neural networks",
      "metadata": { "topic": "machine-learning" },
      "similarity": 0.91
    }
  }
}
```

**Error Responses**

| Scenario | Status | Error |
|----------|--------|-------|
| `topic` param missing or blank | `400 Bad Request` | `"Topic in the search cannot be blank"` |
| Downstream service returns 4xx/5xx | `502 Bad Gateway` | `"Downstream error: <status_code>"` |
| Downstream service unreachable | `503 Service Unavailable` | `"Search service is unavailable"` |
| Unexpected server error | `500 Internal Server Error` | `"An unexpected error occurred"` |

All errors return:
```json
{ "status": 400, "error": "Bad Request", "message": "..." }
```

---

## Key Concepts & Learnings

### Spring Cloud Stream over raw Kafka

Spring Cloud Stream adds a binding abstraction on top of Kafka — you declare a named binding (`ai-producer`) and point it at a destination (topic). This decouples the application code from the Kafka API entirely. Swapping the binder (e.g., from Kafka to RabbitMQ) would require zero code changes.

### Reactive programming with WebFlux and Mono

`RetrieveService` returns a `Mono<Response>` — a reactive type representing a future single value. The handler chains `.flatMap()` on it to build the HTTP response without ever blocking a thread. Under the event-loop model of WebFlux, this means the thread is freed while waiting for the downstream HTTP call to complete.

### Functional routing vs annotation-based controllers

Spring WebFlux supports two programming models. This project uses the functional model (`RouterFunction` + handler classes like `RetrieveInfo`) instead of `@RestController`. Functional routing makes the route definitions explicit and composable — routes are data, not scattered annotations.

### Global error handling with `AbstractErrorWebExceptionHandler`

Rather than `@ExceptionHandler` methods on each controller, this project has a single `GlobalErrorHandler` that intercepts all errors at the WebFlux level. The `@Order(-2)` places it above Spring Boot's default error handler. This produces consistent JSON error shapes across every error type, including Spring-internal exceptions like `NoResourceFoundException`.

### WebClient for non-blocking downstream calls

`WebClient` is the reactive replacement for `RestTemplate`. The `.onStatus()` hook maps HTTP error statuses from the downstream service into typed exceptions (`DownstreamException`). `.onErrorMap()` catches connection failures (`WebClientRequestException`) and wraps them in a `ServiceUnavailableException` — keeping downstream failure modes distinct from application logic failures.

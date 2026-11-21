# Changelog

## 1.11.0 — 2026-11-21

- Declare independent typed `Task[Parameters, Result](name=...)` contracts for
  function schedulers and projections. Bind handlers with `router.task(reference)`
  or `router.projection(reference, read=..., transform=..., write=...)` without
  `payload=` or a shared `dependency=` service.
- Support no-input jobs and multiple serialized inputs alongside native
  `FromDishka` dependencies. References keep typed enqueue/time/cron/interval
  calls; scheduler decorators preserve directly callable original handlers.
- Accept native `schedule=` labels on scheduler functions. Existing discovery,
  class/function APIs, native Taskiq execution, request scopes, retry, projection
  hooks and transport/source ownership remain supported.

## 1.10.0 — 2026-11-19

- Require FastStream 0.7.8 for Events, including its corrected bound Redis List
  batch publisher API. No new runtime dependency family is added.
- Add independent typed `publisher()` declarations for RabbitMQ, Kafka,
  NATS/JetStream and Redis Streams, Pub/Sub and Lists. Existing class publishers
  and `router.publisher()` declarations remain supported.
- Discover standalone publications in existing publisher roots, including
  Outbox discovery; accept explicit typed publications in Events `publishers=`.
- Add optional `publisher=` defaults to function subscribers, preserving native
  consumer settings, explicit patterns/topics/bindings and final addresses under
  router prefixes. Consumer-only registration does not bind a producer.
- Expose shared native router settings explicitly. Bootstrap publication overrides
  use the existing binding owner; references in that application see its effective
  options. Connection configuration, retention, retry and DI ownership stay native.

## 1.9.1 — 2026-11-18

- Move native Dishka callable adaptation, SQL dead-letter management and SQL
  observations to their integration owners. Existing import paths remain valid.
- Share Taskiq container cleanup and beat construction across schedulers and
  projections; separate retry beat dispatch and Outbox relay lifecycle from
  configuration/route assembly without changing native delivery semantics.
- Bind payload-first injected handlers directly with `functools.partial`,
  avoiding repeated general argument binding. Preserve preceding-dependency
  Events handlers, native request scopes, typed payloads and legacy declarations.

## 1.9.0 — 2026-11-18

- Native Dishka parameter injection for function routers: multiple `FromDishka`
  services and native component annotations use the existing request scope,
  cache, cleanup and fresh retry-attempt scopes. Events handlers retain their
  original directly callable function and infer the unmarked event DTO.
- Explicit `payload=YourPayload` contracts for Taskiq function tasks and
  projections preserve typed enqueue/time/cron/interval payloads and results
  while excluding injected services from message signatures. Projection stages
  can request different services using the same scope and shared pipeline.
- Existing class APIs, plain function ParamSpec declarations and the 1.8
  `dependency=Service` form remain supported. Providers/discovery and native
  transports, retry, hooks, monitoring and lifecycle owners are unchanged.

## 1.8.1 — 2026-11-17

- Centralize Events publisher binding and ownership in Registrar for both
  function routers and all class transport registrars. Routers no longer
  access private registrar state or maintain a separate binding path.
- Reject closed/duplicate registrations before native sender creation; retain
  publisher hooks, metadata, error propagation and release/rebinding behavior.
- Verify independent function Scheduler and Projection jobs can wait
  concurrently with separate request-scoped dependencies.

## 1.8.0 — 2026-11-17

- Optional function TaskRouter declarations with typed enqueue/time/cron/interval
  helpers, explicit leading service injection and existing Taskiq execution,
  retry source/beat and monitoring. Broker selection stays in bootstrap.
- Function ProjectionRouter declarations sharing the class read/transform/write
  runner, request scopes, stage hooks, publication hooks and result handling.
- FastStream transport routers for RabbitMQ, Kafka, NATS/JetStream and Redis
  Streams/Lists/PubSub, using native routers and the existing Events retry,
  injection, subscribe hooks and acknowledgement constraints.
- Typed function publisher routes supported by direct publish, transactional
  Outbox append/batch/decorators and native relay, with stable route keys.
- Bootstrapper router discovery in existing family modules/packages, explicit
  routers composition, overlapping-root/re-export deduplication and producer-only
  discovery. Existing classes/import paths remain supported; base dependencies
  and host-owned transaction/lifecycle boundaries are unchanged.

## 1.7.0 — 2026-11-17

- Optional `inbox-postgres` integration using native `sqlalchemy-inbox` 0.2:
  public Inbox, schema and explicit cleanup exports, with no parallel SQL or
  ledger. The native consumer scope/message key shares the host transaction.
- Optional `Monitoring.inbox(session, table)` returns a native subclass that
  observes claim decisions, duplicates, failures/cancellation and durations;
  event IDs remain span attributes. Claim success is distinct from host commit.
- PostgreSQL concurrency, rollback/savepoint, database error, cancellation and
  worker-exit acceptance checks; native FastAPI/Papilio session sharing, NATS
  local retry/redelivery and atomic Inbox/business/Outbox handoff coverage.
- Claim outcome/latency dashboard panels, native strict typing and framework-
  independent installation. No automatic cleanup, schema migration, ack policy,
  external side-effect idempotency or mandatory base dependency is introduced.

## 1.6.0 — 2026-11-17

- Framework-independent `DeadLetters` management for native Outbox PostgreSQL
  tables: queue-filtered cursor pages, archive details and deliberate replay.
- Replay atomically moves one DLQ row into pending with exact payload, headers,
  event/correlation identity and timer key. PostgreSQL serializes concurrent
  replays; host transactions own commit, rollback and timer-conflict recovery.
- Typed metadata excludes payloads from list pages and exposes the native
  delivery count. Native archives do not retain the failed-attempt count.
- Optional shared Monitoring records SQL operation counts, durations and traces;
  dashboard panels cover management activity. Successful SQL is distinct from
  host commit or downstream delivery.
- Real PostgreSQL rollback/savepoint, competing replay, native archive-to-relay,
  custom schema and FastAPI/Papilio integration coverage. No new schema,
  transport dependency, management endpoint or automatic replay/retention job.

## 1.5.1 — 2026-11-17

- Allow database connection setup in the native Outbox lag sampling test on
  hosted runners. Runtime monitoring and transport behavior are unchanged.

## 1.5.0 — 2026-11-17

- Optional OpenTelemetry monitoring for scheduler/projection, Events and Outbox
  using shared host-owned providers and native instrumentation.
- NATS-safe text telemetry headers and real NATS-only scheduler/projection,
  shared KV retry-source and Events monitoring coverage without Redis.
- Idempotent broker instrumentation, stable execution/event trace lookup,
  native batch links and distinct local retry/durable relay outcome counters.
- Taskiq attempt spans close even when retries omit result storage; failed-send
  propagation does not retain a producer context. FastStream restores caller
  context through failures/cancellation. Existing retry/ack behavior is retained.
- Packaged local Collector/Prometheus/Tempo/Grafana stack, provisioned dashboard,
  example alerts and native Outbox lag/freshness configuration.
- Base dependencies remain empty. Monitoring owns no HTTP service, persistence,
  providers, replay engine or application framework. DLQ management/Inbox remain
  separate capabilities.

## 1.4.0 — 2026-11-17

### Added

- Optional `outbox-postgres` integration over native faststream-outbox 0.21.
  Caller-owned SQLAlchemy sessions/connections register typed events alongside
  domain writes; single and native batch append never commit or publish.
- Module-owned stable `Publisher.outbox_key` declarations, existing discovery,
  explicit registrations and early collision/mutation checks. Ordinary publish
  remains direct; direct and Outbox decorators are separate choices.
- Result-preserving `outbox.publish`/`publish_batch` method decorators using an
  injected writer. Transaction presence/identity checks prevent accidentally
  registering outside the owning operation; nested rollback remains native.
- Explicit native FastStream relay applications with producer startup/cleanup,
  registered-target preflight, native durable retry/DLQ/metrics configuration,
  shared Retry/Backoff policy adaptation and stable per-event identity headers.
- Real PostgreSQL commit/rollback, concurrency, retry/DLQ and native FastAPI /
  Papilio UoW integration checks, plus real Redis delivery and independent CLI
  crash/restart tests. Relay durability is at least once, not exactly once.

## 1.3.0 — 2026-11-17

### Added

- Optional Events `Subscriber.retry` policies through `events-retry` and
  Tenacity. Total per-delivery attempts, exception filters, fixed delays and
  capped/jittered Backoff reuse the existing immutable tools policies.
- Each attempt constructs fresh request-scoped dependencies and runs subscriber
  hooks; cleanup finishes before waiting. Cancellation and native message-control
  exceptions propagate. Only the failing subscriber repeats, without republishing.
- Managed retry rejects ACK_FIRST/MANUAL, including registrar and broker defaults.
  Kafka requires an explicit compatible acknowledgement policy. Native terminal
  handling stays selected by the caller; broker redelivery starts a new budget.

## 1.2.0 — 2026-11-16

### Added

- Shared Taskiq `RetryRuntime` for schedulers and projections, with an explicit
  native-loop retry beat that routes a shared writable source to each task's
  registered broker. Includes source lifecycle ownership and immediate mode.
- Immutable capped `Backoff` policies with optional full jitter, retaining fixed
  delays, native attempt/error handling and initial publication behavior.
- Scheduler `enqueue` arguments and native result types inferred from `run`.

### Changed

- Missing retry execution resources are checked before worker readiness;
  producer registration and single-attempt policies do not require a source.
  Existing `retry_source` and single-broker beat factories remain available.
- Configured Redis applications accept a shared runtime without creating
  additional readers of its retry source. Redis stream retention remains an
  explicit native transport choice; no automatic stream deletion is added.

## 1.1.0 — 2026-10-30

### Added

- Optional NATS pull JetStream and native Taskiq Kafka backends for schedulers
  and projections: `scheduler-nats`, `scheduler-kafka`, `projection-nats` and
  `projection-kafka`. Shared infra adapters retain caller-configured native
  brokers; matching app classes and registrars preserve DI, hooks, typed single
  and batch jobs, scheduling, publication and execution retry.
- Kafka class/include topic selection using native routing labels, independently
  of native declarations and worker subscriptions. NATS tasks retain the native
  broker's fixed subject, stream and durable pull consumer.
- Configured Redis app factories, producer lifecycle composition, Redis schedule
  inventory and optional module-owned scheduler defaults, previously published
  on main after 1.0.0.

### Compatibility and limits

- Existing APIs and Memory/Rabbit/Redis defaults are retained. Taskiq remains
  0.12; NATS tasks use taskiq-nats 0.6, independently of FastStream NATS Events.
  Base installation still has no runtime dependencies; Taskiq extras do not
  install FastStream or unrelated transports.
- Kafka task extras constrain kafka-python to `>=2.0.2,<2.3`, retaining the
  native task broker's partition-assignment protocol compatibility with aiokafka.
- Schedule storage and result storage remain explicitly selected. Neither Kafka
  nor JetStream makes a database write and publication atomic or makes effects
  exactly once. Native Kafka auto-commit is not acknowledgement after a successful
  job; native Kafka synchronous admin construction/startup remains visible.
- JetStream uses native pull acknowledgement/redelivery. Stream retention,
  durable consumers, repeated effects and external recovery remain caller-owned.

## 1.0.0 — 2026-09-17

First stable release of Papilio's modular background-job package. Install only
the application/backend extras you need; the base package has no runtime
dependencies.

### Included

- Schedulers with application-owned async jobs and Dishka providers, module
  discovery, immediate enqueue, and explicit time/cron/interval scheduling.
  Memory, RabbitMQ and Redis Streams task brokers; memory and Redis sources.
- Typed Projection read/transform/write pipelines, an identity-transform Direct
  base, single or batch arguments, enqueue and project decorators, and backend
  queue selection.
- Typed Events publishers/subscribers and publish decorators through FastStream:
  RabbitMQ, Kafka, Redis Streams/PubSub/Lists, NATS Core and JetStream.
- Provider-resolved pipeline, publication and subscription hooks with explicit
  ordering and failure policy. Per-job Retry policies for Taskiq applications,
  using an explicitly selected writable source and beat.
- One CLI for scheduler/projection workers and beats, and Events consumers.
  Shared apps/infra/tools layout, typed package metadata and optional extras.
- CI with all four real transports, package builds, and CPython 3.13/3.14 jobs.

### Compatibility

The documented Papilio imports, contracts, factory arguments and behavior are
public API for the 1.x series. Internal bindings/helpers and third-party internals
are outside that commitment. Native handles/options follow the installed
Taskiq/FastStream/backend versions and the dependency ranges in package metadata.
See the README migration section for removed pre-1.0 import paths and APIs.

### Known limitations

- Transactions, outbox/inbox, idempotency and reconciliation remain application
  responsibilities. A successful publish does not establish completed processing
  or exactly-once delivery.
- Memory brokers and sources are process-local and not durable. Separate worker
  and beat processes require shared infrastructure. Large due batches still
  incur payload-copy and execution costs.
- Retry requires a writable source and running beat, including for zero delay.
  Hooks do not replace execution retries. A post-send PublishError can mean the
  message was already published; resending may create duplicates.
- RedisSource delegates to taskiq-redis's list source. It offers no atomic replace
  or lookup by ID. In taskiq-redis 1.2.3, recurring/overdue refresh scans and native
  source shutdown pool-cleanup limitations remain; the adapter adds no pool or
  partial-startup cleanup. Prefer a long-lived source per application/process.
- Concrete job/publisher classes have one active runtime binding per process.
  Projections inheriting the optional hooks constructor need an explicit provider
  factory when using empty hooks; constructor dependencies remain user-owned.
- Redis Pub/Sub is ephemeral; Redis Lists lack acknowledgement/redelivery. Select
  the transport whose delivery semantics fit the application.
- Linux and standard CPython are the current CI scope. Other operating systems,
  free-threaded builds, future Python versions, cluster recovery and production
  capacity are not covered by those checks.

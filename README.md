# tiny-trace

> Zero-dependency OpenTelemetry-compatible tracing for Python. Single file, sync + async, decorator-friendly, W3C Trace Context compliant.

```bash
# coming soon
pip install tiny-trace
```

## Why?

Real OTel is 40+ MB and 4+ transitive packages. For a service that just wants "tracing that doesn't hurt," that's overkill. `tiny-trace` is a strict subset of `opentelemetry-api`:

- W3C `traceparent` / `tracestate` propagation (inject / extract)
- 4 samplers: `AlwaysOn`, `AlwaysOff`, `TraceIdRatio`, `ParentBased`
- 3 exporters: `ConsoleExporter`, `InMemoryExporter`, `JSONLExporter`
- Sync + async `start_as_current_span` (context manager)
- `@tracer.span("op")` decorator (auto-detects sync vs async)
- `record_exception`, `set_attribute`, `add_event`, `add_link`
- Context-isolated current span (per asyncio task)

The API is a strict subset of OTel's. Switching to real OTel later is a one-line provider change.

## Usage

```python
import tiny_trace as tt

tracer = tt.Tracer("my-service", exporter=tt.JSONLExporter("spans.jsonl"))

# Decorator form
@tracer.span("user.create")
async def create_user(payload):
    # ...
    return {"id": 42}

# Context manager form (sync)
with tracer.start_as_current_span("work") as span:
    span.set_attribute("user.id", 42)
    # ...

# Async context manager form
async with tracer.start_as_current_span_async("async-op") as span:
    await do_work()

# W3C propagation (HTTP-style)
carrier = {}
with tracer.start_as_current_span("GET /users") as s:
    tracer.inject(s.context, carrier)
# ... send carrier as 'traceparent' header ...

# On the receiving end:
parent_ctx = tracer.extract(headers)
with tracer.start_as_current_span("handle_users", parent=parent_ctx) as span:
    # span shares trace_id with the original
    pass
```

## Sampling

```python
# 10% of traces
tracer = tt.Tracer("svc", sampler=tt.TraceIdRatio(0.1))

# Follow parent's decision, or sample 50% if root
tracer = tt.Tracer("svc", sampler=tt.ParentBased(tt.TraceIdRatio(0.5)))

# Drop everything
tracer = tt.Tracer("svc", sampler=tt.AlwaysOff())
```

## Exporters

```python
# JSONL file (one span per line)
tt.JSONLExporter("spans.jsonl")

# In-memory (for tests)
exp = tt.InMemoryExporter()
tt.Tracer("svc", exporter=exp)
spans = exp.get_finished_spans()

# Console (stderr)
tt.ConsoleExporter()
```

## What's in the box

| Component | LOC | Notes |
|---|---|---|
| `Tracer` | ~50 | Start spans, inject/extract, decorator factory |
| `Span` | ~80 | Attributes, events, links, status, exception recording |
| `Sampler` ×4 | ~60 | `AlwaysOn`, `AlwaysOff`, `TraceIdRatio`, `ParentBased` |
| `Exporter` ×3 | ~50 | `Console`, `InMemory`, `JSONL` |
| `Propagator` ×1 | ~50 | W3C Trace Context (traceparent / tracestate) |
| `Context` | ~30 | contextvars-based, asyncio-safe |

Total: **~330 LOC**, single file, zero dependencies, 57 tests.

## Ecosystem

Part of the [tiny-* stack](https://github.com/hussain-alsaibai):

| Category | Repo |
|---|---|
| HTTP | [tiny-router](https://github.com/hussain-alsaibai/tiny-router) |
| Logging | [tiny-log](https://github.com/hussain-alsaibai/tiny-log) |
| Validation | [tiny-validator](https://github.com/hussain-alsaibai/tiny-validator) |
| Config | [tiny-config](https://github.com/hussain-alsaibai/tiny-config) |
| CLI | [tiny-cli](https://github.com/hussain-alsaibai/tiny-cli) |
| Cache | [fast-cache](https://github.com/hussain-alsaibai/fast-cache) |
| Rate | [tiny-rate](https://github.com/hussain-alsaibai/tiny-rate) |
| Retry | [tiny-retry](https://github.com/hussain-alsaibai/tiny-retry) |
| Pool | [tiny-pool](https://github.com/hussain-alsaibai/tiny-pool) |
| Compose | [tiny-compose](https://github.com/hussain-alsaibai/tiny-compose) |
| Trace | **tiny-trace** (this) |
| Secret | [tiny-secret](https://github.com/hussain-alsaibai/tiny-secret) |
| AI Agent | [tiny-agent](https://github.com/hussain-alsaibai/tiny-agent) |
| MCP | [tiny-mcp](https://github.com/hussain-alsaibai/tiny-mcp) |
| Embeddings | [tiny-embed](https://github.com/hussain-alsaibai/tiny-embed) |
| Storage | [snapdb](https://github.com/hussain-alsaibai/snapdb) |
| Cron | [tiny-cron](https://github.com/hussain-alsaibai/tiny-cron) |
| Flags | [tiny-flags](https://github.com/hussain-alsaibai/tiny-flags) |
| Queue | [tiny-queue](https://github.com/hussain-alsaibai/tiny-queue) |

## License

MIT — see [LICENSE](LICENSE).

## Today's siblings

- [`tiny-metrics`](https://github.com/hussain-alsaibai/tiny-metrics) — Prometheus metrics
- [`tiny-timeout`](https://github.com/hussain-alsaibai/tiny-timeout) — timeouts that work
- [`tiny-idempotency`](https://github.com/hussain-alsaibai/tiny-idempotency) — idempotency keys

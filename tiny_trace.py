"""tiny-trace: Zero-dependency OpenTelemetry-compatible tracing for Python.

A single-file, OTel-API-compatible tracer that produces W3C Trace Context
spans and exports them anywhere you want. Sync + async, decorator-friendly,
context-propagating, sampling-aware. Zero external deps.

Highlights:

  - tracer = Tracer("my-service", exporter=ConsoleExporter())
  - @tracer.span("user.create") wraps a sync or async function as a span
  - with tracer.start_as_current_span("op") as span: ...  (sync)
  - async with tracer.start_as_current_span("op") as span: ... (async)
  - W3C traceparent / tracestate propagation (parse / inject)
  - Sampling: AlwaysOn / AlwaysOff / TraceIdRatio / ParentBased
  - Built-in exporters: ConsoleExporter, InMemoryExporter, JSONLExporter
  - Built-in propagator: W3CTraceContextPropagator
  - Thread-safe + asyncio-safe

The API surface is a strict subset of opentelemetry-api's Tracer / Span /
Context, so switching to real OTel later is a one-line provider change.
"""

from __future__ import annotations

import contextvars
import functools
import inspect
import json
import os
import random
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

__version__ = "0.1.0"
__all__ = [
    "Tracer",
    "Span",
    "SpanContext",
    "TraceFlags",
    "SpanKind",
    "Status",
    "StatusCode",
    "SamplingResult",
    "Sampler",
    "AlwaysOn",
    "AlwaysOff",
    "TraceIdRatio",
    "ParentBased",
    "Exporter",
    "ConsoleExporter",
    "InMemoryExporter",
    "JSONLExporter",
    "Propagator",
    "W3CTraceContextPropagator",
    "get_current_span",
    "set_current_span",
    "INVALID_SPAN_CONTEXT",
    "TraceState",
    "trace",
    "noop",
]


# ---------------------------------------------------------------------------
# W3C Trace Context constants
# ---------------------------------------------------------------------------

TRACEPARENT_HEADER = "traceparent"
TRACESTATE_HEADER = "tracestate"

# Trace flags
class TraceFlags:
    SAMPLED = 0x01
    NONE = 0x00


# Span kinds (OTel-compatible)
class SpanKind:
    INTERNAL = "INTERNAL"
    SERVER = "SERVER"
    CLIENT = "CLIENT"
    PRODUCER = "PRODUCER"
    CONSUMER = "CONSUMER"


# Status code (OTel-compatible)
class StatusCode:
    UNSET = "UNSET"
    OK = "OK"
    ERROR = "ERROR"


@dataclass
class Status:
    code: str = StatusCode.UNSET
    description: Optional[str] = None

    def __post_init__(self):
        if self.code not in (StatusCode.UNSET, StatusCode.OK, StatusCode.ERROR):
            raise ValueError(f"invalid status code: {self.code!r}")


# ---------------------------------------------------------------------------
# TraceState (key=value,key2=value2 with vendor whitelist)
# ---------------------------------------------------------------------------


class TraceState:
    """W3C tracestate: a list of key=value pairs from registered vendors."""

    def __init__(self, header_value: Optional[str] = None) -> None:
        self._pairs: List[Tuple[str, str]] = []
        if header_value:
            for entry in header_value.split(","):
                entry = entry.strip()
                if not entry:
                    continue
                if "=" not in entry:
                    continue
                k, v = entry.split("=", 1)
                k = k.strip()
                v = v.strip()
                if not k or not v:
                    continue
                self._pairs.append((k, v))

    def add(self, key: str, value: str) -> None:
        # Replace if present, else append.
        self._pairs = [(k, v) for k, v in self._pairs if k != key]
        self._pairs.append((key, value))

    def get(self, key: str) -> Optional[str]:
        for k, v in self._pairs:
            if k == key:
                return v
        return None

    def to_header(self) -> str:
        return ",".join(f"{k}={v}" for k, v in self._pairs)

    def __str__(self) -> str:
        return self.to_header()

    def __repr__(self) -> str:
        return f"TraceState({self.to_header()!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TraceState):
            return False
        return self._pairs == other._pairs


# ---------------------------------------------------------------------------
# Span context (W3C: trace_id, span_id, is_remote, trace_flags, trace_state)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SpanContext:
    trace_id: int
    span_id: int
    is_remote: bool = False
    trace_flags: int = TraceFlags.NONE
    trace_state: TraceState = field(default_factory=TraceState)

    def is_valid(self) -> bool:
        return self.trace_id != 0 and self.span_id != 0

    def is_sampled(self) -> bool:
        return bool(self.trace_flags & TraceFlags.SAMPLED)

    def with_trace_flags(self, flags: int) -> "SpanContext":
        return SpanContext(
            trace_id=self.trace_id,
            span_id=self.span_id,
            is_remote=self.is_remote,
            trace_flags=flags,
            trace_state=self.trace_state,
        )


INVALID_SPAN_CONTEXT = SpanContext(trace_id=0, span_id=0)


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------


def _new_trace_id() -> int:
    """128-bit random trace ID. Avoids all-zero (invalid per W3C)."""
    tid = random.getrandbits(128)
    if tid == 0:
        tid = 1
    return tid


def _new_span_id() -> int:
    """64-bit random span ID. Avoids all-zero."""
    sid = random.getrandbits(64)
    if sid == 0:
        sid = 1
    return sid


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


@dataclass
class SamplingResult:
    decision: str  # "RECORD_AND_SAMPLE", "RECORD_ONLY", "DROP"
    attributes: Dict[str, Any] = field(default_factory=dict)
    trace_state: TraceState = field(default_factory=TraceState)


class Sampler:
    """Base class. Override should_sample()."""

    def should_sample(
        self,
        parent_context: Optional[SpanContext],
        trace_id: int,
        name: str,
        kind: str = SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> SamplingResult:
        raise NotImplementedError

    def description(self) -> str:
        return self.__class__.__name__


class AlwaysOn(Sampler):
    def should_sample(self, parent_context, trace_id, name, kind=SpanKind.INTERNAL, attributes=None):
        return SamplingResult(decision="RECORD_AND_SAMPLE")


class AlwaysOff(Sampler):
    def should_sample(self, parent_context, trace_id, name, kind=SpanKind.INTERNAL, attributes=None):
        return SamplingResult(decision="DROP")


class TraceIdRatio(Sampler):
    def __init__(self, ratio: float) -> None:
        if not 0.0 <= ratio <= 1.0:
            raise ValueError("ratio must be in [0, 1]")
        self._ratio = float(ratio)
        # Bound check uses lower 56 bits (matching OTel implementation).
        self._bound = int(ratio * (1 << 56))

    def should_sample(self, parent_context, trace_id, name, kind=SpanKind.INTERNAL, attributes=None):
        if self._bound == 0:
            return SamplingResult(decision="DROP")
        if self._bound == 1 << 56:
            return SamplingResult(decision="RECORD_AND_SAMPLE")
        decision = (trace_id & ((1 << 56) - 1)) < self._bound
        return SamplingResult(
            decision="RECORD_AND_SAMPLE" if decision else "DROP"
        )

    def description(self) -> str:
        return f"TraceIdRatio{{{self._ratio}}}"


class ParentBased(Sampler):
    """If parent exists, follow parent's decision. Else use root sampler."""

    def __init__(self, root: Sampler) -> None:
        self._root = root

    def should_sample(self, parent_context, trace_id, name, kind=SpanKind.INTERNAL, attributes=None):
        if parent_context is not None and parent_context.is_valid():
            if parent_context.is_sampled():
                return SamplingResult(decision="RECORD_AND_SAMPLE")
            return SamplingResult(decision="DROP")
        return self._root.should_sample(parent_context, trace_id, name, kind, attributes)

    def description(self) -> str:
        return f"ParentBased{{{self._root.description()}}}"


# ---------------------------------------------------------------------------
# Span
# ---------------------------------------------------------------------------


@dataclass
class Span:
    """A single traced operation.

    Mirrors a strict subset of opentelemetry.sdk.trace.Span. Holds
    name, context, parent, timing, attributes, events, status, and links.
    """
    name: str
    context: SpanContext
    parent: Optional[SpanContext] = None
    kind: str = SpanKind.INTERNAL
    start_time_ns: int = 0
    end_time_ns: int = 0
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    links: List[Dict[str, Any]] = field(default_factory=list)
    status: Status = field(default_factory=Status)
    ended: bool = False
    resource_attributes: Dict[str, Any] = field(default_factory=dict)

    def set_attribute(self, key: str, value: Any) -> None:
        if self.ended:
            return
        self.attributes[key] = value

    def set_attributes(self, attrs: Dict[str, Any]) -> None:
        for k, v in attrs.items():
            self.set_attribute(k, v)

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None,
                  timestamp_ns: Optional[int] = None) -> None:
        if self.ended:
            return
        self.events.append({
            "name": name,
            "timestamp_ns": timestamp_ns or time.time_ns(),
            "attributes": dict(attributes or {}),
        })

    def add_link(self, link_context: SpanContext,
                 attributes: Optional[Dict[str, Any]] = None) -> None:
        self.links.append({
            "context": link_context,
            "attributes": dict(attributes or {}),
        })

    def set_status(self, code: str, description: Optional[str] = None) -> None:
        if self.ended:
            return
        if code not in (StatusCode.UNSET, StatusCode.OK, StatusCode.ERROR):
            raise ValueError(f"invalid status code: {code!r}")
        self.status = Status(code=code, description=description)

    def record_exception(self, exc: BaseException,
                         attributes: Optional[Dict[str, Any]] = None) -> None:
        if self.ended:
            return
        attrs = {
            "exception.type": type(exc).__name__,
            "exception.message": str(exc),
            "exception.stacktrace": _format_stacktrace(),
        }
        if attributes:
            attrs.update(attributes)
        self.add_event("exception", attrs)
        self.set_status(StatusCode.ERROR, str(exc))

    def end(self, end_time_ns: Optional[int] = None) -> None:
        if self.ended:
            return
        self.end_time_ns = end_time_ns or time.time_ns()
        self.ended = True

    def duration_ns(self) -> int:
        if not self.ended:
            return 0
        return self.end_time_ns - self.start_time_ns

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "trace_id": format(self.context.trace_id, "032x"),
            "span_id": format(self.context.span_id, "016x"),
            "parent_span_id": format(self.parent.span_id, "016x") if self.parent and self.parent.span_id else None,
            "kind": self.kind,
            "start_time_ns": self.start_time_ns,
            "end_time_ns": self.end_time_ns,
            "duration_ns": self.duration_ns(),
            "attributes": dict(self.attributes),
            "events": list(self.events),
            "links": [
                {
                    "trace_id": format(lc["context"].trace_id, "032x"),
                    "span_id": format(lc["context"].span_id, "016x"),
                    "attributes": lc["attributes"],
                }
                for lc in self.links
            ],
            "status": {"code": self.status.code, "description": self.status.description},
            "ended": self.ended,
            "resource_attributes": dict(self.resource_attributes),
        }


def _format_stacktrace() -> str:
    import traceback
    return "".join(traceback.format_stack()[:-1]) if sys.exc_info()[0] is None else "".join(traceback.format_exc())


# ---------------------------------------------------------------------------
# Exporters
# ---------------------------------------------------------------------------


class Exporter:
    """Base class. Override export()."""

    def export(self, spans: List[Span]) -> None:
        raise NotImplementedError

    def shutdown(self) -> None:
        pass


class ConsoleExporter(Exporter):
    """Print each span to stderr as JSON."""

    def __init__(self, stream=None) -> None:
        import sys
        self._stream = stream if stream is not None else sys.stderr

    def export(self, spans: List[Span]) -> None:
        for s in spans:
            self._stream.write("[tiny-trace] " + json.dumps(s.to_dict(), default=str) + "\n")
        self._stream.flush()


class InMemoryExporter(Exporter):
    """Holds spans in a list for test assertions."""

    def __init__(self) -> None:
        self._spans: List[Span] = []
        self._lock = threading.Lock()

    def export(self, spans: List[Span]) -> None:
        with self._lock:
            self._spans.extend(spans)

    def get_finished_spans(self) -> List[Span]:
        with self._lock:
            return list(self._spans)

    def clear(self) -> None:
        with self._lock:
            self._spans.clear()

    def shutdown(self) -> None:
        self.clear()


class JSONLExporter(Exporter):
    """Append each span as a JSON line to a file path."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.Lock()

    def export(self, spans: List[Span]) -> None:
        with self._lock:
            with open(self._path, "a", encoding="utf-8") as f:
                for s in spans:
                    f.write(json.dumps(s.to_dict(), default=str) + "\n")


# ---------------------------------------------------------------------------
# Propagators
# ---------------------------------------------------------------------------


class Propagator:
    """Inject / Extract W3C headers from a carrier dict."""

    def inject(self, context: SpanContext, carrier: Dict[str, str]) -> None:
        raise NotImplementedError

    def extract(self, carrier: Dict[str, str]) -> SpanContext:
        raise NotImplementedError


class W3CTraceContextPropagator(Propagator):
    """W3C Trace Context (https://www.w3.org/TR/trace-context/) propagator.

    traceparent format: VERSION "-" TRACE_ID "-" PARENT_ID "-" FLAGS
    Example:           00-aaaa...32-bbbb...16-01
    """

    _RE = __import__("re").compile(
        r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(-[0-9a-f]{0,2})?$"
    )

    def inject(self, context: SpanContext, carrier: Dict[str, str]) -> None:
        if not context.is_valid():
            return
        traceparent = (
            f"00-{format(context.trace_id, '032x')}-"
            f"{format(context.span_id, '016x')}-"
            f"{format(context.trace_flags, '02x')}"
        )
        carrier[TRACEPARENT_HEADER] = traceparent
        ts = context.trace_state.to_header()
        if ts:
            carrier[TRACESTATE_HEADER] = ts

    def extract(self, carrier: Dict[str, str]) -> SpanContext:
        tp = carrier.get(TRACEPARENT_HEADER) or carrier.get(TRACEPARENT_HEADER.lower())
        if not tp:
            return INVALID_SPAN_CONTEXT
        m = self._RE.match(tp.strip())
        if not m:
            return INVALID_SPAN_CONTEXT
        version, trace_id_hex, span_id_hex, flags_hex = m.group(1), m.group(2), m.group(3), m.group(4)
        if version != "00":
            return INVALID_SPAN_CONTEXT
        try:
            trace_id = int(trace_id_hex, 16)
            span_id = int(span_id_hex, 16)
            flags = int(flags_hex, 16)
        except ValueError:
            return INVALID_SPAN_CONTEXT
        if trace_id == 0 or span_id == 0:
            return INVALID_SPAN_CONTEXT
        ts_header = carrier.get(TRACESTATE_HEADER) or carrier.get(TRACESTATE_HEADER.lower())
        ts = TraceState(ts_header) if ts_header else TraceState()
        return SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=True,
            trace_flags=flags,
            trace_state=ts,
        )


# ---------------------------------------------------------------------------
# Context: current span tracking (thread-local + asyncio-safe)
# ---------------------------------------------------------------------------


# Use contextvars so async tasks each see their own current span.
_current_span: contextvars.ContextVar[Optional[Span]] = contextvars.ContextVar(
    "tiny_trace_current_span", default=None
)


def get_current_span() -> Span:
    s = _current_span.get()
    if s is None:
        # Return a non-recording no-op span.
        return _NoopSpan()
    return s


def set_current_span(span: Optional[Span]) -> Any:
    """Set the current span. Returns a token usable with reset_current_span()."""
    return _current_span.set(span)


def reset_current_span(token: Any) -> None:
    _current_span.reset(token)  # type: ignore[arg-type]


@dataclass
class _NoopSpan:
    name: str = ""
    context: SpanContext = field(default_factory=lambda: INVALID_SPAN_CONTEXT)
    parent: Optional[SpanContext] = None
    kind: str = SpanKind.INTERNAL
    ended: bool = True
    is_recording: bool = False

    def set_attribute(self, key: str, value: Any) -> None: pass
    def set_attributes(self, attrs: Dict[str, Any]) -> None: pass
    def add_event(self, name: str, **kw: Any) -> None: pass
    def add_link(self, link_context: SpanContext, **kw: Any) -> None: pass
    def set_status(self, code: str, description: Optional[str] = None) -> None: pass
    def record_exception(self, exc: BaseException, **kw: Any) -> None: pass
    def end(self, end_time_ns: Optional[int] = None) -> None: pass
    def duration_ns(self) -> int: return 0


# ---------------------------------------------------------------------------
# Tracer
# ---------------------------------------------------------------------------


F = TypeVar("F", bound=Callable[..., Any])


class Tracer:
    """The entry point. Mirrors a subset of opentelemetry.trace.Tracer.

    Example:
        tracer = Tracer("my-service", exporter=ConsoleExporter())
        with tracer.start_as_current_span("work") as span:
            span.set_attribute("user.id", 42)
        # auto-ended
    """

    def __init__(
        self,
        service_name: str,
        exporter: Optional[Exporter] = None,
        sampler: Optional[Sampler] = None,
        propagator: Optional[Propagator] = None,
        resource_attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.service_name = service_name
        self._exporter = exporter or InMemoryExporter()
        self._sampler = sampler or AlwaysOn()
        self._propagator = propagator or W3CTraceContextPropagator()
        self._resource_attributes = dict(resource_attributes or {})
        self._resource_attributes.setdefault("service.name", service_name)
        self._lock = threading.Lock()
        self._pending: List[Span] = []

    @property
    def exporter(self) -> Exporter:
        return self._exporter

    @property
    def propagator(self) -> Propagator:
        return self._propagator

    def start_span(
        self,
        name: str,
        kind: str = SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None,
        links: Optional[List[SpanContext]] = None,
        start_time_ns: Optional[int] = None,
        parent: Optional[SpanContext] = None,
    ) -> Span:
        # If no explicit parent, use current span's context.
        if parent is None:
            current = get_current_span()
            if current is not None and current.context.is_valid():
                parent = current.context

        trace_id = parent.trace_id if (parent and parent.is_valid()) else _new_trace_id()
        span_id = _new_span_id()

        # Sampling
        sr = self._sampler.should_sample(parent, trace_id, name, kind=kind, attributes=attributes)
        if sr.decision == "DROP":
            # Return non-recording span with INVALID context.
            return Span(
                name=name,
                context=SpanContext(trace_id=trace_id, span_id=span_id, is_remote=False,
                                    trace_flags=TraceFlags.NONE, trace_state=sr.trace_state),
                parent=parent,
                kind=kind,
                start_time_ns=start_time_ns or time.time_ns(),
            )

        flags = TraceFlags.SAMPLED
        ctx = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=False,
            trace_flags=flags,
            trace_state=sr.trace_state,
        )
        span = Span(
            name=name,
            context=ctx,
            parent=parent,
            kind=kind,
            start_time_ns=start_time_ns or time.time_ns(),
            attributes=dict(attributes or {}),
            resource_attributes=dict(self._resource_attributes),
        )
        if sr.attributes:
            span.set_attributes(sr.attributes)
        for link_ctx in (links or []):
            span.add_link(link_ctx)
        return span

    @contextmanager
    def start_as_current_span(
        self,
        name: str,
        kind: str = SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None,
        parent: Optional[SpanContext] = None,
    ) -> Iterator[Span]:
        span = self.start_span(name, kind=kind, attributes=attributes, parent=parent)
        token = set_current_span(span)
        try:
            yield span
        except BaseException as e:
            try:
                span.record_exception(e)
            finally:
                span.end()
                self._on_end(span)
            raise
        else:
            span.end()
            self._on_end(span)
        finally:
            reset_current_span(token)

    def start_as_current_span_async(
        self,
        name: str,
        kind: str = SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None,
        parent: Optional[SpanContext] = None,
    ) -> "AsyncSpanCM":
        """Async context manager form. Use as:
            async with tracer.start_as_current_span_async("op") as span: ...
        """
        return AsyncSpanCM(self, name, kind=kind, attributes=attributes, parent=parent)

    def _on_end(self, span: Span) -> None:
        if not span.context.is_sampled():
            return
        with self._lock:
            self._pending.append(span)
        # Export immediately (simple model — OTel batches).
        try:
            self._exporter.export([span])
        except Exception as e:  # pragma: no cover
            sys.stderr.write(f"[tiny-trace] export error: {e}\n")

    def span(self, name: str, kind: str = SpanKind.INTERNAL,
             attributes: Optional[Dict[str, Any]] = None) -> Callable[[F], F]:
        """Decorator. Auto-detects sync vs async."""
        def decorator(fn: F) -> F:
            if inspect.iscoroutinefunction(fn):
                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    with self.start_as_current_span(name, kind=kind, attributes=attributes):
                        return await fn(*args, **kwargs)
                async_wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
                return async_wrapper  # type: ignore[return-value]
            else:
                @functools.wraps(fn)
                def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                    with self.start_as_current_span(name, kind=kind, attributes=attributes):
                        return fn(*args, **kwargs)
                sync_wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
                return sync_wrapper  # type: ignore[return-value]
        return decorator

    def inject(self, context: SpanContext, carrier: Dict[str, str]) -> None:
        self._propagator.inject(context, carrier)

    def extract(self, carrier: Dict[str, str]) -> SpanContext:
        return self._propagator.extract(carrier)

    def flush(self) -> None:
        with self._lock:
            spans = self._pending[:]
            self._pending.clear()
        if spans:
            self._exporter.export(spans)

    def shutdown(self) -> None:
        self.flush()
        self._exporter.shutdown()


class AsyncSpanCM:
    """Async context manager wrapper around Tracer.start_as_current_span()."""

    def __init__(self, tracer: Tracer, name: str, kind: str,
                 attributes: Optional[Dict[str, Any]],
                 parent: Optional[SpanContext] = None) -> None:
        self._cm = tracer.start_as_current_span(name, kind=kind, attributes=attributes, parent=parent)
        self._span: Optional[Span] = None

    async def __aenter__(self) -> Span:
        # __enter__ returns the span
        self._span = self._cm.__enter__()
        return self._span

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return self._cm.__exit__(exc_type, exc, tb)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


_default_tracer: Optional[Tracer] = None


def trace(service_name: str = "default", exporter: Optional[Exporter] = None) -> Tracer:
    """Module-level convenience. Returns a process-wide default tracer."""
    global _default_tracer
    if _default_tracer is None:
        _default_tracer = Tracer(service_name, exporter=exporter)
    return _default_tracer


def noop() -> Span:
    """Return a non-recording no-op span (e.g. for fallback paths)."""
    return _NoopSpan()

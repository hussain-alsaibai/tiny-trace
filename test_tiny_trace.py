"""Tests for tiny-trace — run with `python test_tiny_trace.py`. Stdlib only."""

import asyncio
import json
import os
import re
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import tiny_trace as tt


# ---------------------------------------------------------------------------
# SpanContext & TraceState
# ---------------------------------------------------------------------------


class TestSpanContext(unittest.TestCase):
    def test_invalid(self):
        self.assertFalse(tt.INVALID_SPAN_CONTEXT.is_valid())

    def test_valid(self):
        ctx = tt.SpanContext(trace_id=1, span_id=2)
        self.assertTrue(ctx.is_valid())
        self.assertFalse(ctx.is_sampled())

    def test_sampled_flag(self):
        ctx = tt.SpanContext(trace_id=1, span_id=2, trace_flags=tt.TraceFlags.SAMPLED)
        self.assertTrue(ctx.is_sampled())

    def test_with_trace_flags(self):
        ctx = tt.SpanContext(trace_id=1, span_id=2, trace_flags=tt.TraceFlags.NONE)
        ctx2 = ctx.with_trace_flags(tt.TraceFlags.SAMPLED)
        self.assertTrue(ctx2.is_sampled())
        self.assertFalse(ctx.is_sampled())


class TestTraceState(unittest.TestCase):
    def test_empty(self):
        ts = tt.TraceState()
        self.assertEqual(ts.to_header(), "")

    def test_parse_single(self):
        ts = tt.TraceState("vendor1=val1")
        self.assertEqual(ts.get("vendor1"), "val1")

    def test_parse_multiple(self):
        ts = tt.TraceState("a=1,b=2,c=3")
        self.assertEqual(ts.get("a"), "1")
        self.assertEqual(ts.get("b"), "2")
        self.assertEqual(ts.get("c"), "3")

    def test_add_replaces(self):
        ts = tt.TraceState("a=1")
        ts.add("a", "2")
        self.assertEqual(ts.get("a"), "2")
        self.assertEqual(ts.to_header(), "a=2")

    def test_invalid_entries_skipped(self):
        ts = tt.TraceState("a=1,invalid,bad,=noval,noval=")
        self.assertEqual(ts.to_header(), "a=1")

    def test_equality(self):
        self.assertEqual(tt.TraceState("a=1"), tt.TraceState("a=1"))
        self.assertNotEqual(tt.TraceState("a=1"), tt.TraceState("a=2"))


# ---------------------------------------------------------------------------
# Samplers
# ---------------------------------------------------------------------------


class TestSamplers(unittest.TestCase):
    def test_always_on(self):
        s = tt.AlwaysOn()
        r = s.should_sample(None, 12345, "test")
        self.assertEqual(r.decision, "RECORD_AND_SAMPLE")

    def test_always_off(self):
        s = tt.AlwaysOff()
        r = s.should_sample(None, 12345, "test")
        self.assertEqual(r.decision, "DROP")

    def test_ratio_zero(self):
        s = tt.TraceIdRatio(0.0)
        r = s.should_sample(None, 12345, "test")
        self.assertEqual(r.decision, "DROP")

    def test_ratio_one(self):
        s = tt.TraceIdRatio(1.0)
        r = s.should_sample(None, 12345, "test")
        self.assertEqual(r.decision, "RECORD_AND_SAMPLE")

    def test_ratio_mid(self):
        s = tt.TraceIdRatio(0.5)
        # Use random 128-bit trace IDs, not sequential integers in [0, 1000)
        # — those would all be in the lower half of the 56-bit bound.
        import random
        n_sampled = sum(
            1 for _ in range(1000)
            if s.should_sample(None, random.getrandbits(128), "test").decision == "RECORD_AND_SAMPLE"
        )
        # Allow generous bounds for statistical noise
        self.assertGreater(n_sampled, 350)
        self.assertLess(n_sampled, 650)

    def test_ratio_invalid(self):
        with self.assertRaises(ValueError):
            tt.TraceIdRatio(1.5)
        with self.assertRaises(ValueError):
            tt.TraceIdRatio(-0.1)

    def test_parent_based_with_sampled_parent(self):
        s = tt.ParentBased(tt.AlwaysOff())
        parent = tt.SpanContext(trace_id=1, span_id=2, trace_flags=tt.TraceFlags.SAMPLED)
        r = s.should_sample(parent, 1, "test")
        self.assertEqual(r.decision, "RECORD_AND_SAMPLE")

    def test_parent_based_with_unsampled_parent(self):
        s = tt.ParentBased(tt.AlwaysOn())
        parent = tt.SpanContext(trace_id=1, span_id=2, trace_flags=tt.TraceFlags.NONE)
        r = s.should_sample(parent, 1, "test")
        self.assertEqual(r.decision, "DROP")

    def test_parent_based_no_parent_uses_root(self):
        s = tt.ParentBased(tt.AlwaysOff())
        r = s.should_sample(None, 1, "test")
        self.assertEqual(r.decision, "DROP")


# ---------------------------------------------------------------------------
# Span
# ---------------------------------------------------------------------------


class TestSpan(unittest.TestCase):
    def _make_span(self, name="op", start_time_ns=None):
        return tt.Span(
            name=name,
            context=tt.SpanContext(trace_id=1, span_id=2, trace_flags=tt.TraceFlags.SAMPLED),
            start_time_ns=start_time_ns or 0,
        )

    def test_set_attribute(self):
        s = self._make_span()
        s.set_attribute("user.id", 42)
        self.assertEqual(s.attributes["user.id"], 42)

    def test_set_attribute_after_end_is_noop(self):
        s = self._make_span()
        s.end()
        s.set_attribute("k", "v")
        self.assertNotIn("k", s.attributes)

    def test_set_attributes(self):
        s = self._make_span()
        s.set_attributes({"a": 1, "b": 2})
        self.assertEqual(s.attributes, {"a": 1, "b": 2})

    def test_add_event(self):
        s = self._make_span()
        s.add_event("checkpoint", {"stage": 1})
        self.assertEqual(len(s.events), 1)
        self.assertEqual(s.events[0]["name"], "checkpoint")
        self.assertEqual(s.events[0]["attributes"], {"stage": 1})

    def test_add_event_after_end(self):
        s = self._make_span()
        s.end()
        s.add_event("after")
        self.assertEqual(s.events, [])

    def test_set_status(self):
        s = self._make_span()
        s.set_status(tt.StatusCode.OK)
        self.assertEqual(s.status.code, tt.StatusCode.OK)

    def test_set_status_invalid(self):
        s = self._make_span()
        with self.assertRaises(ValueError):
            s.set_status("BOGUS")

    def test_record_exception(self):
        s = self._make_span()
        try:
            raise ValueError("nope")
        except ValueError as e:
            s.record_exception(e)
        self.assertEqual(s.status.code, tt.StatusCode.ERROR)
        self.assertEqual(s.status.description, "nope")
        self.assertEqual(len(s.events), 1)
        self.assertEqual(s.events[0]["name"], "exception")
        self.assertEqual(s.events[0]["attributes"]["exception.type"], "ValueError")

    def test_end_records_time(self):
        s = self._make_span(start_time_ns=time.time_ns())
        time.sleep(0.001)
        s.end()
        self.assertGreater(s.end_time_ns, s.start_time_ns)
        self.assertGreater(s.duration_ns(), 0)

    def test_end_idempotent(self):
        s = self._make_span()
        s.end()
        first_end = s.end_time_ns
        s.end()
        self.assertEqual(s.end_time_ns, first_end)

    def test_to_dict(self):
        s = self._make_span()
        s.set_attribute("k", "v")
        s.end()
        d = s.to_dict()
        self.assertEqual(d["name"], "op")
        self.assertEqual(d["trace_id"], format(1, "032x"))
        self.assertEqual(d["span_id"], format(2, "016x"))
        self.assertEqual(d["attributes"], {"k": "v"})
        self.assertTrue(d["ended"])

    def test_add_link(self):
        s = self._make_span()
        other = tt.SpanContext(trace_id=99, span_id=88)
        s.add_link(other, {"rel": "follows_from"})
        self.assertEqual(len(s.links), 1)
        self.assertEqual(s.links[0]["attributes"], {"rel": "follows_from"})


# ---------------------------------------------------------------------------
# Exporters
# ---------------------------------------------------------------------------


class TestExporters(unittest.TestCase):
    def test_in_memory(self):
        exp = tt.InMemoryExporter()
        span = tt.Span(name="x", context=tt.SpanContext(trace_id=1, span_id=2))
        span.end()
        exp.export([span])
        self.assertEqual(len(exp.get_finished_spans()), 1)
        exp.clear()
        self.assertEqual(len(exp.get_finished_spans()), 0)

    def test_in_memory_thread_safe(self):
        exp = tt.InMemoryExporter()

        def add_spans(n):
            for i in range(n):
                s = tt.Span(name=f"s{i}", context=tt.SpanContext(trace_id=1, span_id=i + 1))
                s.end()
                exp.export([s])

        threads = [threading.Thread(target=add_spans, args=(50,)) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(exp.get_finished_spans()), 250)

    def test_console(self):
        import io
        buf = io.StringIO()
        exp = tt.ConsoleExporter(stream=buf)
        span = tt.Span(name="console-op", context=tt.SpanContext(trace_id=1, span_id=2))
        span.end()
        exp.export([span])
        out = buf.getvalue()
        self.assertIn("console-op", out)
        # Must be valid JSON line
        line = out.strip().split("\n")[0]
        line = line.replace("[tiny-trace] ", "", 1)
        d = json.loads(line)
        self.assertEqual(d["name"], "console-op")

    def test_jsonl(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "spans.jsonl")
            exp = tt.JSONLExporter(path)
            span1 = tt.Span(name="a", context=tt.SpanContext(trace_id=1, span_id=2))
            span1.end()
            span2 = tt.Span(name="b", context=tt.SpanContext(trace_id=1, span_id=3))
            span2.end()
            exp.export([span1, span2])
            with open(path) as f:
                lines = [l for l in f if l.strip()]
            self.assertEqual(len(lines), 2)
            self.assertEqual(json.loads(lines[0])["name"], "a")
            self.assertEqual(json.loads(lines[1])["name"], "b")


# ---------------------------------------------------------------------------
# Propagator
# ---------------------------------------------------------------------------


class TestPropagator(unittest.TestCase):
    def setUp(self):
        self.prop = tt.W3CTraceContextPropagator()

    def test_inject_extract_roundtrip(self):
        ctx = tt.SpanContext(trace_id=0xAAAA * (1 << 96), span_id=0xBBBB * (1 << 48),
                             trace_flags=tt.TraceFlags.SAMPLED)
        carrier: dict = {}
        self.prop.inject(ctx, carrier)
        self.assertIn("traceparent", carrier)
        extracted = self.prop.extract(carrier)
        self.assertEqual(extracted.trace_id, ctx.trace_id)
        self.assertEqual(extracted.span_id, ctx.span_id)
        self.assertEqual(extracted.trace_flags, ctx.trace_flags)
        self.assertTrue(extracted.is_remote)

    def test_extract_missing(self):
        extracted = self.prop.extract({})
        self.assertFalse(extracted.is_valid())

    def test_extract_malformed(self):
        for bad in ["", "00-aaaa-bbbb-cc", "ff-aaaa-bbbb-cc", "00-0-bbbb-01", "00-aaaa-0-01"]:
            extracted = self.prop.extract({"traceparent": bad})
            self.assertFalse(extracted.is_valid(), f"should be invalid: {bad!r}")

    def test_extract_with_tracestate(self):
        ctx = tt.SpanContext(trace_id=1, span_id=2, trace_flags=tt.TraceFlags.SAMPLED,
                             trace_state=tt.TraceState("vendor=value"))
        carrier: dict = {}
        self.prop.inject(ctx, carrier)
        extracted = self.prop.extract(carrier)
        self.assertEqual(extracted.trace_state.get("vendor"), "value")

    def test_inject_invalid_does_nothing(self):
        carrier: dict = {}
        self.prop.inject(tt.INVALID_SPAN_CONTEXT, carrier)
        self.assertEqual(carrier, {})


# ---------------------------------------------------------------------------
# Tracer
# ---------------------------------------------------------------------------


class TestTracer(unittest.TestCase):
    def setUp(self):
        self.exp = tt.InMemoryExporter()
        self.tracer = tt.Tracer("test-svc", exporter=self.exp)

    def test_start_span_sets_context(self):
        with self.tracer.start_as_current_span("op") as s:
            self.assertTrue(s.context.is_valid())
            self.assertTrue(s.context.is_sampled())
            self.assertEqual(s.name, "op")

    def test_start_span_attributes(self):
        with self.tracer.start_as_current_span("op", attributes={"a": 1}) as s:
            self.assertEqual(s.attributes["a"], 1)

    def test_ends_on_exit(self):
        with self.tracer.start_as_current_span("op") as s:
            pass
        self.assertTrue(s.ended)

    def test_records_exception(self):
        try:
            with self.tracer.start_as_current_span("op") as s:
                raise ValueError("nope")
        except ValueError:
            pass
        spans = self.exp.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].status.code, tt.StatusCode.ERROR)
        self.assertEqual(len(spans[0].events), 1)
        self.assertEqual(spans[0].events[0]["name"], "exception")

    def test_child_uses_parent_trace_id(self):
        with self.tracer.start_as_current_span("parent") as p:
            with self.tracer.start_as_current_span("child") as c:
                self.assertEqual(c.context.trace_id, p.context.trace_id)
                self.assertNotEqual(c.context.span_id, p.context.span_id)

    def test_resource_attributes_on_spans(self):
        tracer = tt.Tracer("svc", exporter=tt.InMemoryExporter(),
                           resource_attributes={"service.version": "1.2.3"})
        with tracer.start_as_current_span("op") as s:
            self.assertEqual(s.resource_attributes["service.name"], "svc")
            self.assertEqual(s.resource_attributes["service.version"], "1.2.3")

    def test_span_decorator_sync(self):
        @self.tracer.span("work")
        def do_work(x):
            return x * 2
        result = do_work(21)
        self.assertEqual(result, 42)
        spans = self.exp.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "work")

    def test_span_decorator_async(self):
        @self.tracer.span("async-work")
        async def do_work(x):
            return x * 2
        result = asyncio.run(do_work(21))
        self.assertEqual(result, 42)
        spans = self.exp.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "async-work")

    def test_span_decorator_records_exception(self):
        @self.tracer.span("boom")
        def bad():
            raise RuntimeError("nope")
        with self.assertRaises(RuntimeError):
            bad()
        spans = self.exp.get_finished_spans()
        self.assertEqual(spans[0].status.code, tt.StatusCode.ERROR)

    def test_dropped_sample_not_exported(self):
        tracer = tt.Tracer("svc", exporter=tt.InMemoryExporter(), sampler=tt.AlwaysOff())
        with tracer.start_as_current_span("op") as s:
            pass
        # Span was created but not sampled
        self.assertFalse(s.context.is_sampled())
        # No export
        self.assertEqual(len(tracer.exporter.get_finished_spans()), 0)

    def test_sampled_parent_with_parent_based(self):
        parent = tt.SpanContext(trace_id=1, span_id=2, trace_flags=tt.TraceFlags.SAMPLED)
        tracer = tt.Tracer("svc", exporter=tt.InMemoryExporter(),
                           sampler=tt.ParentBased(tt.AlwaysOff()))
        span = tracer.start_span("child", parent=parent)
        self.assertTrue(span.context.is_sampled())

    def test_async_context_manager(self):
        async def main():
            async with self.tracer.start_as_current_span_async("async-op") as s:
                self.assertFalse(s.ended)
                self.assertTrue(s.context.is_valid())
            return s

        s = asyncio.run(main())
        self.assertTrue(s.ended)

    def test_async_decorator_with_exception(self):
        @self.tracer.span("async-boom")
        async def bad():
            raise RuntimeError("nope")
        with self.assertRaises(RuntimeError):
            asyncio.run(bad())
        spans = self.exp.get_finished_spans()
        self.assertEqual(spans[0].status.code, tt.StatusCode.ERROR)

    def test_inject_extract_via_tracer(self):
        with self.tracer.start_as_current_span("op") as s:
            carrier: dict = {}
            self.tracer.inject(s.context, carrier)
        extracted = self.tracer.extract(carrier)
        self.assertEqual(extracted.trace_id, s.context.trace_id)
        self.assertTrue(extracted.is_remote)

    def test_nested_decorators(self):
        @self.tracer.span("outer")
        @self.tracer.span("inner")
        def f():
            return "ok"
        self.assertEqual(f(), "ok")
        spans = self.exp.get_finished_spans()
        # Both spans should be exported.
        names = sorted(s.name for s in spans)
        self.assertEqual(names, ["inner", "outer"])
        inner = next(s for s in spans if s.name == "inner")
        outer = next(s for s in spans if s.name == "outer")
        # When stacked `@outer @inner def f`:
        # - `outer_wrapper` runs first, starts span_outer, calls inner_wrapper
        # - `inner_wrapper` runs, starts span_inner with parent=span_outer
        # So outer is the parent of inner.
        self.assertIsNone(outer.parent)  # outer is the root
        self.assertEqual(inner.parent.span_id, outer.context.span_id)
        # Both share a trace_id
        self.assertEqual(outer.context.trace_id, inner.context.trace_id)


# ---------------------------------------------------------------------------
# Context propagation across async tasks
# ---------------------------------------------------------------------------


class TestAsyncContext(unittest.TestCase):
    def test_current_span_isolated_per_task(self):
        tracer = tt.Tracer("svc", exporter=tt.InMemoryExporter())
        results = {}

        async def child():
            with tracer.start_as_current_span("child") as s:
                results["child"] = s.context.span_id
                await asyncio.sleep(0.01)
                # When we re-enter, current span should still be ours
                current = tt.get_current_span()
                results["child_re"] = current.context.span_id

        async def parent():
            with tracer.start_as_current_span("parent") as p:
                results["parent"] = p.context.span_id
                await child()
                # After child returns, current span should be back to parent
                current = tt.get_current_span()
                results["parent_re"] = current.context.span_id

        asyncio.run(parent())
        self.assertEqual(results["child"], results["child_re"])
        self.assertEqual(results["parent"], results["parent_re"])


# ---------------------------------------------------------------------------
# End-to-end: HTTP-style trace propagation
# ---------------------------------------------------------------------------


class TestE2E(unittest.TestCase):
    def test_client_to_server(self):
        client_tracer = tt.Tracer("client", exporter=tt.InMemoryExporter())
        server_tracer = tt.Tracer("server", exporter=tt.InMemoryExporter())

        # Client makes a request, injects headers.
        carrier: dict = {}
        with client_tracer.start_as_current_span("GET /users") as client_span:
            client_tracer.inject(client_span.context, carrier)

        # Server receives the request, extracts headers, creates child span.
        server_parent = server_tracer.extract(carrier)
        with server_tracer.start_as_current_span("handle_users", parent=server_parent) as server_span:
            self.assertEqual(server_span.context.trace_id, client_span.context.trace_id)
            self.assertEqual(server_span.parent.span_id, client_span.context.span_id)


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""End to end: the real OpenTelemetry SDK exports OTLP/HTTP protobuf to `newdb serve`, and
the answers are checked over the HTTP API (crates/newdb-server/docs/api.md), for both
`--memory` and a real data directory. With a data directory they are also checked through
this module, read-only next to the running server and again after it stops."""

import json
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

import newdb

REPO = Path(__file__).resolve().parents[2]
PROJECT = "e2e"


@pytest.fixture(scope="module")
def newdb_bin():
    subprocess.run(["cargo", "build", "-q", "-p", "newdb-cli"], cwd=REPO, check=True)
    return REPO / "target" / "debug" / "newdb"


def start_server(newdb_bin, store_args):
    proc = subprocess.Popen(
        [newdb_bin, "serve", *store_args, "--addr", "127.0.0.1:0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    line = proc.stdout.readline()
    if not line.startswith("listening on http://"):
        proc.kill()
        raise AssertionError(f"newdb serve did not start: {line!r} {proc.stderr.read()}")
    return proc, line.split()[-1]


def stop_server(proc):
    proc.terminate()
    proc.wait(timeout=10)


def get(base, path):
    with urllib.request.urlopen(base + path, timeout=10) as r:
        return json.load(r)


def export_two_runs(base):
    """Exports two agent runs; returns their trace IDs as hex, oldest first."""
    provider = TracerProvider(
        resource=Resource.create({"service.name": "e2e-service", "newdb.project": PROJECT})
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=base + "/v1/traces")))
    tracer = provider.get_tracer("newdb-e2e")

    with tracer.start_as_current_span("invoke_agent planner") as root:
        root.set_attribute("gen_ai.operation.name", "invoke_agent")
        root.set_attribute("session.id", "sess-1")
        with tracer.start_as_current_span("chat gpt-5") as chat:
            chat.set_attribute("gen_ai.operation.name", "chat")
            chat.set_attribute("gen_ai.request.model", "gpt-5")
            chat.set_attribute("gen_ai.provider.name", "openai")
            chat.set_attribute("gen_ai.usage.input_tokens", 120)
            chat.set_attribute("gen_ai.usage.output_tokens", 30)
            chat.set_attribute(
                "gen_ai.input.messages",
                json.dumps([{"role": "user", "parts": [{"type": "text", "content": "What is the refund policy?"}]}]),
            )
            chat.set_attribute(
                "gen_ai.output.messages",
                json.dumps([{"role": "assistant", "parts": [{"type": "text", "content": "Thirty days."}]}]),
            )
        with tracer.start_as_current_span("execute_tool lookup_order") as tool:
            tool.set_attribute("gen_ai.operation.name", "execute_tool")
            tool.set_attribute("gen_ai.tool.call.arguments", '{"order": 42}')
            tool.set_status(Status(StatusCode.ERROR, "order service timed out"))
    first = root.get_span_context().trace_id

    time.sleep(0.002)  # strictly later start time for the second run
    with tracer.start_as_current_span("invoke_agent triage") as root2:
        root2.set_attribute("gen_ai.operation.name", "invoke_agent")
        root2.set_attribute("session.id", "sess-2")
        root2.set_status(Status(StatusCode.ERROR, "gave up"))
    second = root2.get_span_context().trace_id

    assert provider.force_flush(10_000)
    provider.shutdown()
    return f"{first:032x}", f"{second:032x}"


def check_over_http(base, first, second):
    assert get(base, "/healthz") == {"status": "ok"}

    trace = get(base, f"/v1/traces/{first}?project={PROJECT}&stats=1")
    assert trace["project"] == PROJECT and trace["trace_id"] == first
    [root] = trace["spans"]
    assert root["name"] == "invoke_agent planner" and root["parent_span_id"] is None
    assert root["run_type"] == "agent" and root["session"] == "sess-1"
    assert root["attributes"]["resource"]["service.name"] == "e2e-service"
    chat, tool = root["children"]  # store order: by start time
    assert chat["name"] == "chat gpt-5" and chat["parent_span_id"] == root["span_id"]
    assert chat["run_type"] == "chat_model"
    assert (chat["model"], chat["provider"]) == ("gpt-5", "openai")
    assert (chat["prompt_tokens"], chat["completion_tokens"]) == (120, 30)
    assert chat["cost_micro_usd"] == 120 * 1.25 + 30 * 10  # gpt-5 list price, micro-USD
    assert "refund policy" in json.dumps(chat["inputs"])
    assert "Thirty days." in json.dumps(chat["outputs"])
    assert tool["run_type"] == "tool" and tool["status"] == "error"
    assert tool["error"] == "order service timed out"
    assert int(root["start_ns"]) <= int(chat["start_ns"]) <= int(tool["start_ns"])
    assert int(root["end_ns"]) >= int(tool["end_ns"])
    assert trace["stats"]["records_returned"] == 3

    lean = get(base, f"/v1/traces/{first}?project={PROJECT}&payloads=false")
    assert lean["spans"][0]["children"][0]["inputs"] is None

    with pytest.raises(urllib.error.HTTPError) as e:
        get(base, f"/v1/traces/{first}?project=other")
    assert e.value.code == 404

    runs = get(base, f"/v1/runs?project={PROJECT}")
    assert [(r["trace_id"], r["name"]) for r in runs["runs"]] == [
        (second, "invoke_agent triage"),
        (first, "invoke_agent planner"),
    ]
    assert runs["next_cursor"] is None
    newest = runs["runs"][0]
    assert newest["status"] == "error" and newest["session"] == "sess-2"
    assert int(newest["latency_ns"]) == int(newest["end_ns"]) - int(newest["start_ns"])

    errors = get(base, f"/v1/runs?project={PROJECT}&status=error")
    assert [r["trace_id"] for r in errors["runs"]] == [second]
    by_session = get(base, f"/v1/runs?project={PROJECT}&session=sess-1&run_type=agent")
    assert [r["trace_id"] for r in by_session["runs"]] == [first]

    page1 = get(base, f"/v1/runs?project={PROJECT}&limit=1")
    page2 = get(base, f"/v1/runs?project={PROJECT}&limit=1&cursor={page1['next_cursor']}")
    assert [r["trace_id"] for r in page1["runs"] + page2["runs"]] == [second, first]

    hits = get(base, f"/v1/search?project={PROJECT}&q=Refund+POLICY")["hits"]
    assert [(h["trace_id"], h["span_id"]) for h in hits] == [(first, chat["span_id"])]
    assert get(base, "/v1/search?project=other&q=refund")["hits"] == []
    return {"chat": chat["span_id"], "tool": tool["span_id"], "root": root["span_id"]}


def check_through_module(db, first, second, ids):
    spans = {s["span_id"]: s for s in db.trace(PROJECT, first)}
    assert set(spans) == set(ids.values())
    chat = spans[ids["chat"]]
    assert (chat["model"], chat["prompt_tokens"], chat["cost_micro_usd"]) == ("gpt-5", 120, 450)
    assert b"refund policy" in chat["inputs"]
    assert spans[ids["tool"]]["status"] == "error"
    assert [r["trace_id"] for r in db.runs(project=PROJECT)] == [second, first]
    assert [r["trace_id"] for r in db.runs(project=PROJECT, status="error")] == [second]
    assert [h["span_id"] for h in db.search("refund policy", project=PROJECT)] == [ids["chat"]]


def test_otel_sdk_to_serve_memory(newdb_bin):
    proc, base = start_server(newdb_bin, ["--memory"])
    try:
        first, second = export_two_runs(base)
        check_over_http(base, first, second)
    finally:
        stop_server(proc)


def test_otel_sdk_to_serve_data_dir(newdb_bin, tmp_path):
    data = tmp_path / "db"
    proc, base = start_server(newdb_bin, ["--data", str(data)])
    try:
        first, second = export_two_runs(base)
        ids = check_over_http(base, first, second)
        # A read-only open next to the running writer sees every acknowledged span.
        check_through_module(newdb.Db.open(str(data), read_only=True), first, second, ids)
    finally:
        stop_server(proc)

    # Acknowledged spans survive the server being killed; the writer lock is released.
    db = newdb.Db.open(str(data))
    check_through_module(db, first, second, ids)
    del db
    out = subprocess.run([newdb_bin, "verify", "--data", str(data)], capture_output=True, text=True)
    assert out.returncode == 0 and out.stdout.endswith("0 problems\n"), out

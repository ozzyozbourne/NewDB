"""The `newdb` module against the in-memory store (`Db.memory()`, the oracle).

The same calls work on `Db.open(path)`; test_engine.py runs them there once the engine lands.
"""

import random
import re

import pytest

import newdb

T1 = "0af7651916cd43dd8448eb211c80319c"
T2 = "1bf7651916cd43dd8448eb211c80319d"
ROOT = "b7ad6b7169203331"
CHILD = "00000000000000a1"


def test_start_and_end_posts_merge_and_payloads_come_back_as_sent():
    db = newdb.Db.memory()
    db.ingest(
        [
            {
                "project": "p",
                "trace_id": T1,
                "span_id": ROOT,
                "parent_span_id": "0000000000000000",
                "name": "agent",
                "start_ns": 1_000,
                "status": "unset",
                "run_type": "agent",
                "session": "s1",
                "inputs": b'{"q": "Refund policy?"}',
                "attributes": '{"user": "\u00e9"}',
            },
            {
                "project": "p",
                "trace_id": T1,
                "span_id": CHILD,
                "parent_span_id": ROOT,
                "name": "chat",
                "start_ns": 1_500,
                "end_ns": 1_900,
                "run_type": "chat_model",
                "model": "gpt-6-sol",
                "provider": "openai",
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "cost_micro_usd": 42,
            },
        ]
    )
    db.ingest(
        [
            {
                "project": "p",
                "trace_id": T1,
                "span_id": ROOT,
                "end_ns": 9_000,
                "status": "error",
                "outputs": b"{}",
                "error": "boom",
                "name": None,  # None means "not sent": the name stays.
            }
        ]
    )

    root, child = db.trace("p", T1)
    assert root == {
        "project": "p",
        "trace_id": T1,
        "span_id": ROOT,
        "parent_span_id": None,
        "name": "agent",
        "start_ns": 1_000,
        "end_ns": 9_000,
        "status": "error",
        "run_type": "agent",
        "model": None,
        "provider": None,
        "session": "s1",
        "prompt_tokens": None,
        "completion_tokens": None,
        "cost_micro_usd": None,
        "inputs": b'{"q": "Refund policy?"}',
        "outputs": b"{}",
        "attributes": '{"user": "\u00e9"}'.encode(),
        "events": None,
        "error": "boom",
        "version": 3,
    }
    assert child["parent_span_id"] == ROOT
    assert (child["model"], child["prompt_tokens"], child["cost_micro_usd"]) == ("gpt-6-sol", 10, 42)

    lean = db.trace("p", T1, payloads=False)
    assert [s["span_id"] for s in lean] == [ROOT, CHILD]
    assert lean[0]["inputs"] is None and lean[0]["outputs"] is None
    assert lean[0]["attributes"] is None and lean[0]["error"] == "boom"

    assert db.trace("other-project", T1) == []
    assert db.trace("p", T2) == []

    [run] = db.runs()
    assert run["span_id"] == ROOT and run["latency_ns"] == 8_000
    assert run["status"] == "error" and run["run_type"] == "agent"
    assert run["prompt_tokens"] == 0  # the root's own value, not the trace total

    assert [h["span_id"] for h in db.search("REFUND policy")] == [ROOT]
    assert db.search("refund policy", project="other") == []


def test_a_span_read_back_can_be_posted_again():
    db = newdb.Db.memory()
    db.ingest([{"project": "p", "trace_id": T1, "span_id": ROOT, "start_ns": 3}])
    [span] = db.trace("p", T1)
    db.ingest([span])
    [again] = db.trace("p", T1)
    assert again["version"] == 2
    assert {**again, "version": 1} == span


def test_stats_true_returns_value_and_stats():
    db = newdb.Db.memory()
    db.ingest([{"project": "p", "trace_id": T1, "span_id": ROOT, "inputs": b"hello"}])
    for value, stats in [
        db.trace("p", T1, stats=True),
        db.runs(stats=True),
        db.search("hello", stats=True),
    ]:
        assert len(value) == 1
        assert set(stats) == {
            "partitions_touched",
            "bytes_read",
            "payload_bytes_decompressed",
            "records_visited",
            "records_returned",
            "phases_ns",
            "allocations",
        }
        assert stats["records_returned"] == 1
        assert isinstance(stats["phases_ns"], list)


@pytest.mark.parametrize(
    "span, message",
    [
        ({"trace_id": T1, "span_id": ROOT}, "project is required"),
        ({"project": "p", "span_id": ROOT}, "trace_id is required"),
        ({"project": "p", "trace_id": T1}, "span_id is required"),
        ({"project": "p", "trace_id": T1[:-1], "span_id": ROOT}, "trace_id"),
        ({"project": "p", "trace_id": T1, "span_id": "zz" * 8}, "span_id"),
        ({"project": "p", "trace_id": T1, "span_id": ROOT, "nmae": "x"}, 'unknown key "nmae"'),
        ({"project": "p", "trace_id": T1, "span_id": ROOT, "status": "bad"}, "status"),
        ({"project": "p", "trace_id": T1, "span_id": ROOT, "run_type": "x"}, "run_type"),
        ({"project": "p", "trace_id": T1, "span_id": ROOT, "start_ns": -1}, "start_ns"),
        ({"project": "p", "trace_id": T1, "span_id": ROOT, "prompt_tokens": 2**32}, "prompt_tokens"),
        ({"project": "p", "trace_id": T1, "span_id": ROOT, "inputs": 5}, "inputs"),
    ],
)
def test_invalid_spans_raise_value_error_and_ingest_nothing(span, message):
    db = newdb.Db.memory()
    good = {"project": "p", "trace_id": T2, "span_id": ROOT}
    with pytest.raises(ValueError, match=re.escape(message)):
        db.ingest([good, span])
    assert db.runs() == []


def test_invalid_queries_raise_value_error():
    db = newdb.Db.memory()
    with pytest.raises(ValueError, match="not a dict"):
        db.ingest(["span"])
    with pytest.raises(ValueError, match="trace_id"):
        db.trace("p", "nothex")
    with pytest.raises(ValueError, match="status"):
        db.runs(status="failed")
    with pytest.raises(ValueError, match="run_type"):
        db.runs(run_type="robot")
    with pytest.raises(ValueError, match="before"):
        db.runs(before={"start_ns": 1})
    with pytest.raises(TypeError):
        db.runs("p")  # filters are keyword-only


# A brute-force model of the documented semantics, independent of the Rust code.

WORDS = ["refund", "policy", "order", "Invoice", "delay", "billing", "42", "ACCOUNT"]


def random_posts(rng, traces=40):
    posts, late = [], []
    for t in range(traces):
        if t % 4 == 3:  # deliver closing posts a few traces later, shuffled
            rng.shuffle(late)
            posts += late
            late = []
        trace_id = f"{rng.getrandbits(128):032x}"
        project = f"p{t % 3}"
        ids = [f"{rng.getrandbits(64) | 1:016x}" for _ in range(rng.randint(1, 6))]
        for i, span_id in enumerate(ids):
            start = rng.choice([None, rng.randint(0, 10_000)])
            post = {"project": project, "trace_id": trace_id, "span_id": span_id}
            if i:
                post["parent_span_id"] = rng.choice(ids[:i])
            if start is not None:
                post["start_ns"] = start
            post["status"] = rng.choice(["ok", "error", "unset"])
            post["run_type"] = rng.choice(["agent", "chain", "tool"])
            post["session"] = rng.choice([None, "s1", "s2"])
            post["prompt_tokens"] = rng.randint(0, 100)
            post["inputs"] = " ".join(rng.sample(WORDS, 3)).encode()
            posts.append(post)
            if rng.random() < 0.5:  # a closing post, delivered later
                close = {"project": project, "trace_id": trace_id, "span_id": span_id}
                close["end_ns"] = (start or 0) + rng.randint(0, 5_000)
                close["status"] = rng.choice(["ok", "error"])
                close["outputs"] = rng.choice(WORDS).encode()
                late.append(close)
    return posts + late


def merged(posts):
    spans = {}
    for p in posts:
        key = (p["project"], p["trace_id"], p["span_id"])
        cur = spans.setdefault(key, {})
        cur.update({k: v for k, v in p.items() if v is not None})
    return list(spans.values())


def run_key(s):
    return (s.get("start_ns", 0), s["trace_id"], s["span_id"])


def tokens(b):
    return set(re.findall(rb"[a-z0-9]+", b.lower()))


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_answers_match_a_python_model_of_the_semantics(seed):
    rng = random.Random(seed)
    posts = random_posts(rng)
    db = newdb.Db.memory()
    for i in range(0, len(posts), 7):
        db.ingest(posts[i : i + 7])
    spans = merged(posts)

    roots = sorted((s for s in spans if "parent_span_id" not in s), key=run_key, reverse=True)

    def expect_runs(pred, limit=50):
        return [(s["trace_id"], s["span_id"]) for s in roots if pred(s)][:limit]

    def got(rows):
        return [(r["trace_id"], r["span_id"]) for r in rows]

    assert got(db.runs()) == expect_runs(lambda s: True)
    assert got(db.runs(project="p1", limit=5)) == expect_runs(lambda s: s["project"] == "p1", 5)
    assert got(db.runs(status="error")) == expect_runs(lambda s: s["status"] == "error")
    assert got(db.runs(session="s2", run_type="agent")) == expect_runs(
        lambda s: s.get("session") == "s2" and s["run_type"] == "agent"
    )
    assert got(db.runs(min_latency_ns=2_000)) == expect_runs(
        lambda s: "end_ns" in s and max(s["end_ns"] - s.get("start_ns", 0), 0) >= 2_000
    )
    assert got(db.runs(min_total_tokens=50, start_after_ns=5_000)) == expect_runs(
        lambda s: s["prompt_tokens"] >= 50 and s.get("start_ns", 0) >= 5_000
    )

    # Paging with before= walks every run exactly once, in order.
    pages, page = [], db.runs(limit=4)
    while page:
        pages += page
        page = db.runs(limit=4, before=page[-1])
    assert got(pages) == expect_runs(lambda s: True, limit=len(roots))

    for text, project in [("refund policy", None), ("invoice", "p0"), ("42 account", None), ("", None)]:
        want = tokens(text.encode())
        hits = sorted(
            (
                s
                for s in spans
                if want
                and (project is None or s["project"] == project)
                and want <= tokens(s.get("inputs", b"")) | tokens(s.get("outputs", b""))
            ),
            key=run_key,
            reverse=True,
        )[:10]
        assert got(db.search(text, project=project, limit=10)) == [
            (s["trace_id"], s["span_id"]) for s in hits
        ], text

    for project, trace_id in {(s["project"], s["trace_id"]) for s in spans}:
        want = sorted(
            (s for s in spans if (s["project"], s["trace_id"]) == (project, trace_id)),
            key=lambda s: (s.get("start_ns", 0), s["span_id"]),
        )
        have = db.trace(project, trace_id)
        assert [s["span_id"] for s in have] == [s["span_id"] for s in want]
        for h, w in zip(have, want):
            for k, v in w.items():
                assert h[k] == v, (k, h, w)

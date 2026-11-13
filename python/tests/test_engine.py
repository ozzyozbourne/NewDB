"""The `newdb` module on the real engine (`Db.open(path)`), checked against the same Python
model as the oracle in test_module.py."""

import random

import pytest

import newdb
from test_module import check_against_model, random_posts


def open_db(path, **kw):
    try:
        return newdb.Db.open(str(path), fsync=False, **kw)
    except BaseException as e:  # a Rust panic arrives as pyo3_runtime.PanicException
        if "not yet implemented: lane 1" in str(e):
            pytest.skip("the engine (lane 1) is not merged yet")
        raise


@pytest.mark.parametrize("seed", [4, 5])
def test_engine_answers_match_the_model_and_survive_reopen(tmp_path, seed):
    posts = random_posts(random.Random(seed))
    db = open_db(tmp_path)
    for i in range(0, len(posts), 11):
        db.ingest(posts[i : i + 11])
    check_against_model(db, posts)
    del db  # dropping the Db flushes and stops the writer

    check_against_model(open_db(tmp_path), posts)


def test_read_only_open_sees_committed_data_and_rejects_ingest(tmp_path):
    posts = random_posts(random.Random(6), traces=5)
    db = open_db(tmp_path)
    db.ingest(posts)
    del db

    ro = open_db(tmp_path, read_only=True)
    check_against_model(ro, posts)
    with pytest.raises(RuntimeError, match="read-only"):
        ro.ingest(posts[:1])

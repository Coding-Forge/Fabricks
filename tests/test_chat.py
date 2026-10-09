import io
import json
import sqlite3
import unittest
from types import SimpleNamespace

from chat import engine, source

TENANT = "00000000-0000-0000-0000-000000000001"
CALL = "79e78d95-e6d0-4b25-80b3-5e757508fca6"
PROV = {"tables_listed": 2, "tables_read": 2, "stream_metric_coverage": 1.0, "files": []}


class FakeResponse:
    def __init__(self, payload, headers=None, status=200):
        self.payload, self.headers, self.status_code = payload, headers or {}, status

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise source.requests.HTTPError(str(self.status_code))


class DeltaSession:
    """Serves Delta tables: {table root: {"commits": {name: text}, "parquet": {file: bytes}}}; listing by directory."""

    def __init__(self, tables, extra_dirs=()):
        self.files, self.dirs = {}, {}
        for root, spec in tables.items():
            log = f"{root}/_delta_log"
            self.dirs[log] = [{"name": f"{log}/{n}"} for n in spec["commits"]]
            self.files.update({f"{log}/{n}": c.encode() for n, c in spec["commits"].items()})
            self.files.update({f"{root}/{n}": b for n, b in spec["parquet"].items()})
        for parent, names in extra_dirs:
            self.dirs[parent] = [{"name": f"{parent}/{n}", "isDirectory": "true"} for n in names]

    def get(self, url, params=None, **kwargs):
        if params:
            return FakeResponse({"paths": self.dirs.get(params["directory"], [])})
        response = FakeResponse(None)
        response.content = self.files[url.split("/CQD/")[1]]
        return response


class FakeCredential:
    def get_token(self, scope):
        return SimpleNamespace(token="t")


def parquet_bytes(rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    sink = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), sink)
    return sink.getvalue()


LH = "cqd_lh.Lakehouse/Tables/dbo"


def commit(*actions):
    return "\n".join(json.dumps(a) for a in actions)


class SalesTests(unittest.TestCase):
    def source(self, session):
        return source.ShortcutSource("onelake.example", "CQD", "cqd_lh", TENANT, FakeCredential(), session)

    def session(self, with_checkpoint_gap=False):
        commits = {
            "00000000000000000000.json": commit({"add": {"path": "part-0.parquet"}}),
            "00000000000000000001.json": commit({"remove": {"path": "part-0.parquet"}},
                                                {"add": {"path": "part%2D1.parquet"}}, {"add": {"path": "part-2.parquet"}}),
        }
        if with_checkpoint_gap:
            del commits["00000000000000000000.json"]
        parquet = {
            "part-0.parquet": parquet_bytes([{"id": 99, "region": 9, "amount": 0.9}]),
            "part-1.parquet": parquet_bytes([{"id": 1, "region": 0, "amount": 0.25}]),
            "part-2.parquet": parquet_bytes([{"id": 2, "region": 1, "amount": 0.5}]),
        }
        return DeltaSession({f"{LH}/sales": {"commits": commits, "parquet": parquet}})

    def test_replays_delta_log_and_loads_only_active_files(self):
        conn, prov = source.load(self.source(self.session()))
        self.assertEqual(conn.execute('SELECT id FROM "Sales" ORDER BY id').fetchall(), [(1,), (2,)])
        self.assertEqual((prov["sales_rows"], prov["sales_version"], prov["sales_error"]), (2, 1, None))
        self.assertEqual(len(prov["sales_files"]), 2)

    def test_incomplete_log_is_reported_not_fatal(self):
        conn, prov = source.load(self.source(self.session(with_checkpoint_gap=True)))
        self.assertIn("checkpointed", prov["sales_error"])
        self.assertEqual(prov["sales_rows"], 0)
        self.assertIsNone(conn.execute("SELECT name FROM sqlite_master WHERE name='Sales'").fetchone())


class LayerTests(unittest.TestCase):
    def test_loads_only_silver_and_gold_tables_not_bronze_files(self):
        def table(rows):
            return {"commits": {"00000000000000000000.json": commit({"add": {"path": "p.parquet"}})},
                    "parquet": {"p.parquet": parquet_bytes(rows)}}

        session = DeltaSession(
            {f"{LH}/silver_mediastreams": table([{"CallId": "a", "AvgJitterMs": 4.0}, {"CallId": "b", "AvgJitterMs": None}]),
             f"{LH}/gold_call_summary": table([{"CallId": "a", "AvgJitterMs": 4.0}]),
             f"{LH}/silver_broken": {"commits": {}, "parquet": {}}},
            extra_dirs=[(LH, ["sales", "silver_mediastreams", "gold_call_summary", "silver_broken", "other"])])
        src = source.ShortcutSource("onelake.example", "CQD", "cqd_lh", TENANT, FakeCredential(), session)
        conn, prov = source.load(src, include_sales=False)
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
        self.assertEqual(names, {"silver_mediastreams", "gold_call_summary"})
        self.assertEqual(prov["tables_listed"], 3)
        self.assertEqual(prov["stream_metric_coverage"], 0.5)
        self.assertTrue(all("Files/bronze" not in f["path"] for f in prov["files"]))


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE Calls (CallId, Durations)")
        self.conn.execute("INSERT INTO Calls VALUES ('a', 10), ('b', 20)")

    def test_rejects_writes_and_multiple_statements(self):
        for sql in ("DELETE FROM Calls", "DROP TABLE Calls", "PRAGMA writable_schema=1"):
            self.assertIsNotNone(engine.run_sql(self.conn, sql)["error"])
        self.assertIsNotNone(engine.run_sql(self.conn, "SELECT 1; SELECT 2")["error"])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM Calls").fetchone()[0], 2)

    def test_select_returns_rows(self):
        result = engine.run_sql(self.conn, "SELECT COUNT(*) AS n FROM Calls;")
        self.assertEqual(result["rows"], [{"n": 2}])

    def test_truncation_flag(self):
        result = engine.run_sql(self.conn, "SELECT * FROM Calls", max_rows=1)
        self.assertTrue(result["truncated"])
        self.assertEqual(len(result["rows"]), 1)

    def test_grounded_answer_scores_high(self):
        result = engine.run_sql(self.conn, "SELECT COUNT(*) AS n FROM Calls")
        score = engine.score("There are 2 calls.", [result], PROV)
        self.assertEqual(score["band"], "High")

    def test_ungrounded_number_lowers_score(self):
        result = engine.run_sql(self.conn, "SELECT COUNT(*) AS n FROM Calls")
        good = engine.score("There are 2 calls.", [result], PROV)["value"]
        bad = engine.score("There are 7 calls.", [result], PROV)["value"]
        self.assertLess(bad, good)

    def test_no_query_is_capped_low(self):
        score = engine.score("There are 2 calls.", [], PROV)
        self.assertEqual(score["band"], "Low")
        self.assertLessEqual(score["value"], 20)

    def test_sparse_metrics_reduce_completeness(self):
        result = engine.run_sql(self.conn, "SELECT 5 AS AvgJitterMs")
        sparse = dict(PROV, stream_metric_coverage=0.4)
        full = engine.score("Jitter is 5.", [result], PROV)["value"]
        self.assertLess(engine.score("Jitter is 5.", [result], sparse)["value"], full)

    def test_ask_runs_tool_loop(self):
        replies = iter([
            SimpleNamespace(tool_calls=[SimpleNamespace(
                id="1", function=SimpleNamespace(arguments=json.dumps({"sql": "SELECT COUNT(*) AS n FROM Calls"})))],
                content=None),
            SimpleNamespace(tool_calls=None, content="There are 2 calls."),
        ])
        completions = SimpleNamespace(create=lambda **kw: SimpleNamespace(choices=[SimpleNamespace(message=next(replies))]))
        client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
        out = engine.ask(client, "m", self.conn, PROV, "How many calls?")
        self.assertEqual(out["queries"][0]["row_count"], 1)
        self.assertEqual(out["score"]["band"], "High")


if __name__ == "__main__":
    unittest.main()

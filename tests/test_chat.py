import json
import sqlite3
import unittest
from types import SimpleNamespace

from chat import engine, source

TENANT = "2be9ade8-01e0-4553-8639-e549bb72dbd5"
CALL = "79e78d95-e6d0-4b25-80b3-5e757508fca6"
PROV = {"calls_listed": 2, "calls_read": 2, "stream_metric_coverage": 1.0, "files": []}


class FakeResponse:
    def __init__(self, payload, headers=None, status=200):
        self.payload, self.headers, self.status_code = payload, headers or {}, status

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise source.requests.HTTPError(str(self.status_code))


class FakeSession:
    def __init__(self, listing_pages, files):
        self.pages, self.files, self.urls = list(listing_pages), files, []

    def get(self, url, params=None, **kwargs):
        self.urls.append((url, params))
        if params:
            return self.pages.pop(0)
        return FakeResponse(self.files[url.split("/CQD/")[1]])


class FakeCredential:
    def get_token(self, scope):
        return SimpleNamespace(token="t")


def record(version):
    return {"ingested_at": "2026-10-09T00:00:00+00:00", "record": {
        "id": CALL, "version": version, "type": "groupCall", "modalities": ["audio"],
        "startDateTime": "2026-10-09T10:00:00Z", "endDateTime": "2026-10-09T10:05:00Z",
        "participants_v2": [], "sessions": []}}


class SourceTests(unittest.TestCase):
    def test_reads_latest_version_through_shortcut_path(self):
        root = f"cqd_lh.Lakehouse/Files/bronze/callrecords/{TENANT}/{CALL}"
        pages = [
            FakeResponse({"paths": [{"name": f"{root}/version=1.json"}]}, {"x-ms-continuation": "n"}),
            FakeResponse({"paths": [{"name": f"{root}/version=2.json"}, {"name": root, "isDirectory": "true"}]}),
        ]
        session = FakeSession(pages, {f"{root}/version=2.json": record(2)})
        src = source.ShortcutSource("onelake.example", "CQD", "cqd_lh", TENANT, FakeCredential(), session)
        conn, prov = source.load(src)
        self.assertEqual(prov["files"][0]["version"], 2)
        self.assertIn("Files/bronze/callrecords", prov["shortcut_path"])
        self.assertEqual(conn.execute('SELECT Version FROM "Calls"').fetchone()[0], 2)
        self.assertEqual(session.urls[1][1]["continuation"], "n")


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

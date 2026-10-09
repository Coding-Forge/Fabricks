"""Natural-language Q&A over the shortcut-loaded tables with a transparent, heuristic confidence score."""
import json
import re
import sqlite3

MAX_ROWS = 200
FORBIDDEN = re.compile(r"\b(insert|update|delete|drop|alter|create|attach|detach|pragma|replace|vacuum|reindex)\b", re.I)
METRIC_REFERENCE = re.compile(r"jitter|roundtrip|packetloss|concealed|degradation", re.I)
NUMBER = re.compile(r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?")

TOOL = [{
    "type": "function",
    "function": {
        "name": "run_sql",
        "description": "Run one read-only SQLite SELECT over the call-quality tables.",
        "parameters": {
            "type": "object",
            "properties": {"sql": {"type": "string", "description": "A single SQLite SELECT statement."}},
            "required": ["sql"],
        },
    },
}]


def schema_text(conn):
    lines = []
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        columns = ", ".join(row[1] for row in conn.execute(f'PRAGMA table_info("{name}")'))
        lines.append(f"{name}({columns})")
    return "\n".join(lines)


def system_prompt(conn):
    return (
        "You answer questions about Microsoft Teams call-quality records by calling run_sql.\n"
        "Tables (SQLite dialect, SELECT only):\n" + schema_text(conn) + "\n"
        "Notes: CallId joins Calls, Sessions and Participants; SessionId joins Sessions, Segments and MediaStreams. "
        "Jitter and round-trip values are milliseconds; packet loss is a ratio. Many streams have NULL metrics because "
        "Teams did not report them; say so when it affects an answer. Users are pseudonymous hashes. "
        "State only numbers that appear in query results. If the data cannot answer the question, say so."
    )


def run_sql(conn, sql, max_rows=MAX_ROWS):
    statement = sql.strip().rstrip(";")
    if not statement.lower().startswith(("select", "with")) or FORBIDDEN.search(statement):
        return {"sql": sql, "error": "Rejected: only a single read-only SELECT is allowed.", "rows": [], "truncated": False}
    try:
        conn.execute("PRAGMA query_only=ON")
        cursor = conn.execute(statement)
        columns = [c[0] for c in cursor.description]
        fetched = cursor.fetchmany(max_rows + 1)
    except sqlite3.Error as exc:
        return {"sql": sql, "error": f"SQL error: {exc}", "rows": [], "truncated": False}
    return {
        "sql": statement,
        "error": None,
        "rows": [dict(zip(columns, row)) for row in fetched[:max_rows]],
        "truncated": len(fetched) > max_rows,
    }


def _numbers(text):
    values = set()
    for token in NUMBER.findall(text):
        try:
            values.add(round(float(token.replace(",", "")), 2))
        except ValueError:
            continue
    return values


def score(answer, results, provenance):
    """Heuristic confidence, not a measured accuracy. Each component is 0..1."""
    ran = [r for r in results if r["error"] is None]
    with_rows = [r for r in ran if r["rows"]]
    if not results:
        query = 0.0
    elif not ran:
        query = 0.0
    elif not with_rows:
        query = 0.4
    else:
        query = 1.0 if len(ran) == len(results) else 0.6
    if any(r["truncated"] for r in results):
        query = min(query, 0.6)

    evidence = set()
    for result in with_rows:
        evidence |= _numbers(json.dumps(result["rows"]))
    claimed = _numbers(answer)
    if claimed:
        grounding = sum(1 for n in claimed if any(abs(n - e) <= 0.011 for e in evidence)) / len(claimed)
    else:
        grounding = 0.7 if with_rows else 0.0

    completeness = 1.0
    if provenance["calls_listed"] and provenance["calls_read"] < provenance["calls_listed"]:
        completeness *= provenance["calls_read"] / provenance["calls_listed"]
    if any(METRIC_REFERENCE.search(r["sql"]) for r in results):
        completeness *= max(provenance["stream_metric_coverage"], 0.1)

    value = 0.25 * query + 0.45 * grounding + 0.30 * completeness
    if not with_rows:
        value = min(value, 0.2)
    value = round(100 * value)
    band = "High" if value >= 80 else "Medium" if value >= 50 else "Low"
    return {
        "value": value,
        "band": band,
        "components": {
            "query_success": round(query, 2),
            "answer_grounded_in_results": round(grounding, 2),
            "source_completeness": round(completeness, 2),
        },
        "note": "Heuristic confidence from checks on this answer; it is not a measured accuracy.",
    }


def ask(client, deployment, conn, provenance, question, max_steps=5):
    messages = [{"role": "system", "content": system_prompt(conn)}, {"role": "user", "content": question}]
    results, answer = [], "Stopped after too many query attempts."
    for _ in range(max_steps):
        response = client.chat.completions.create(model=deployment, messages=messages, tools=TOOL)
        message = response.choices[0].message
        if not message.tool_calls:
            answer = message.content or ""
            break
        messages.append(message)
        for call in message.tool_calls:
            try:
                sql = json.loads(call.function.arguments)["sql"]
            except (ValueError, KeyError, TypeError):
                result = {"sql": "", "error": "Malformed tool arguments", "rows": [], "truncated": False}
            else:
                result = run_sql(conn, sql)
            results.append(result)
            messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps(result)[:20000]})
    return {
        "answer": answer,
        "queries": [{"sql": r["sql"], "row_count": len(r["rows"]), "error": r["error"], "truncated": r["truncated"]}
                    for r in results],
        "score": score(answer, results, provenance),
        "source": {k: v for k, v in provenance.items() if k != "files"} | {"files_read": provenance["files"]},
    }

"""Reads Bronze call records through the Lakehouse shortcut over the OneLake API and loads pseudonymized tables."""
import importlib.util
import io
import json
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import unquote
from uuid import UUID

import requests
from azure.identity import AzureAuthorityHosts, ClientSecretCredential

ONELAKE_SCOPE = "https://storage.azure.com/.default"
FILE_PATTERN = re.compile(r"(?P<call>[0-9a-fA-F-]{36})/version=(?P<version>\d+)\.json$")
COMMIT_PATTERN = re.compile(r"(?P<version>\d{20})\.json$")
TABLES = ("Calls", "Participants", "Sessions", "Segments", "MediaStreams")


def _load_flatten():
    path = Path(__file__).resolve().parent.parent / "scripts" / "flatten_samples.py"
    spec = importlib.util.spec_from_file_location("flatten_samples", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.flatten


class ShortcutSource:
    def __init__(self, onelake_host, workspace, lakehouse, tenant_id, credential, session=None,
                 sales_table="dbo/sales"):
        self.base = f"https://{onelake_host}"
        self.sales_root = f"{lakehouse}.Lakehouse/Tables/{sales_table}"
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.tenant_id = str(UUID(tenant_id))
        self.credential = credential
        self.session = session or requests.Session()
        self.root = f"{lakehouse}.Lakehouse/Files/bronze/callrecords/{self.tenant_id}"

    def _headers(self):
        return {"Authorization": f"Bearer {self.credential.get_token(ONELAKE_SCOPE).token}"}

    def latest_files(self):
        """Newest version file per call, discovered by listing the shortcut folder."""
        latest, token = {}, None
        while True:
            params = {"resource": "filesystem", "recursive": "true", "directory": self.root, "maxResults": "5000"}
            if token:
                params["continuation"] = token
            response = self.session.get(f"{self.base}/{self.workspace}", params=params,
                                        headers=self._headers(), timeout=(10, 60), allow_redirects=False)
            if response.status_code == 404:
                return {}
            response.raise_for_status()
            for item in response.json().get("paths", []):
                match = FILE_PATTERN.search(item.get("name", ""))
                if match and str(item.get("isDirectory")).lower() != "true":
                    call, version = match["call"].lower(), int(match["version"])
                    if call not in latest or version > latest[call][0]:
                        latest[call] = (version, item["name"])
            token = response.headers.get("x-ms-continuation")
            if not token:
                return latest

    def read(self, name):
        response = self.session.get(f"{self.base}/{self.workspace}/{name}", headers=self._headers(),
                                    timeout=(10, 90), allow_redirects=False)
        response.raise_for_status()
        return response.json()

    def read_bytes(self, name):
        response = self.session.get(f"{self.base}/{self.workspace}/{name}", headers=self._headers(),
                                    timeout=(10, 90), allow_redirects=False)
        response.raise_for_status()
        return response.content

    def _list(self, directory):
        names, token = [], None
        while True:
            params = {"resource": "filesystem", "recursive": "false", "directory": directory, "maxResults": "5000"}
            if token:
                params["continuation"] = token
            response = self.session.get(f"{self.base}/{self.workspace}", params=params,
                                        headers=self._headers(), timeout=(10, 60), allow_redirects=False)
            if response.status_code == 404:
                return []
            response.raise_for_status()
            names += [p["name"] for p in response.json().get("paths", []) if str(p.get("isDirectory")).lower() != "true"]
            token = response.headers.get("x-ms-continuation")
            if not token:
                return names

    def sales_files(self):
        """Active parquet files of the sales Delta table, found by replaying its _delta_log commits."""
        log_dir = f"{self.sales_root}/_delta_log"
        commits = sorted((m["version"], n) for n in self._list(log_dir) if (m := COMMIT_PATTERN.search(n)))
        commits = [(int(v), n) for v, n in commits]
        if not commits:
            return 0, []
        if commits[0][0] != 0 or any(b[0] != a[0] + 1 for a, b in zip(commits, commits[1:])):
            raise ValueError("Delta log is not a complete commit history (checkpointed table is not supported)")
        active = {}
        for _, name in commits:
            for line in self.read_bytes(name).decode().splitlines():
                action = json.loads(line) if line.strip() else {}
                if "add" in action:
                    active[unquote(action["add"]["path"])] = action["add"]
                elif "remove" in action:
                    active.pop(unquote(action["remove"]["path"]), None)
        if any(a.get("deletionVector") for a in active.values()):
            raise ValueError("Delta deletion vectors are not supported")
        return commits[-1][0], sorted(f"{self.sales_root}/{path}" for path in active)

    def read_sales(self):
        """Returns (rows as dicts, delta version, parquet file paths)."""
        import pyarrow.parquet as pq

        version, files = self.sales_files()
        rows = []
        for name in files:
            rows += pq.read_table(io.BytesIO(self.read_bytes(name))).to_pylist()
        return rows, version, files


def _quote(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def load(source, flatten=None, max_calls=500, include_sales=True):
    """Returns (sqlite connection, provenance). Files are read through the shortcut path."""
    flatten = flatten or _load_flatten()
    files = source.latest_files()
    chosen = sorted(files.items())[-max_calls:]
    snapshots, read_files, failed = [], [], 0
    for call, (version, name) in chosen:
        try:
            envelope = source.read(name)
            snapshots.append({"record": envelope["record"], "retrieved_at": envelope.get("ingested_at")})
            read_files.append({"call": call, "version": version, "path": name})
        except (requests.RequestException, ValueError, KeyError):
            failed += 1
    tables = flatten(snapshots)
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    for table in TABLES:
        rows = tables.get(table) or []
        if not rows:
            continue
        columns = list(rows[0])
        conn.execute(f"CREATE TABLE {_quote(table)} ({', '.join(_quote(c) for c in columns)})")
        conn.executemany(
            f"INSERT INTO {_quote(table)} VALUES ({','.join('?' * len(columns))})",
            [tuple(row[c] for c in columns) for row in rows],
        )
    conn.commit()
    sales = {"sales_path": f"{source.workspace}/{source.sales_root}", "sales_rows": 0, "sales_version": None,
             "sales_files": [], "sales_error": None}
    if include_sales:
        try:
            rows, version, sales_files = source.read_sales()
            if rows:
                columns = list(rows[0])
                conn.execute(f'CREATE TABLE "Sales" ({", ".join(_quote(c) for c in columns)})')
                conn.executemany(
                    f'INSERT INTO "Sales" VALUES ({",".join("?" * len(columns))})',
                    [tuple(row[c] for c in columns) for row in rows],
                )
                conn.commit()
            sales.update(sales_rows=len(rows), sales_version=version, sales_files=sales_files)
        except (requests.RequestException, ValueError, KeyError, OSError) as exc:
            sales["sales_error"] = type(exc).__name__ + (f": {exc}" if isinstance(exc, ValueError) else "")
    streams = tables.get("MediaStreams") or []
    with_metrics = sum(1 for s in streams if s.get("AvgJitterMs") is not None)
    provenance = {
        "shortcut_path": f"{source.workspace}/{source.root}",
        "calls_listed": len(files),
        "calls_read": len(read_files),
        "calls_failed": failed,
        "calls_capped": max(0, len(files) - len(chosen)),
        "files": read_files,
        "stream_rows": len(streams),
        "stream_metric_coverage": (with_metrics / len(streams)) if streams else 1.0,
        "loaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **sales,
    }
    return conn, provenance


def make_credential(tenant_id, client_id, client_secret):
    return ClientSecretCredential(
        tenant_id=str(UUID(tenant_id)),
        client_id=str(UUID(client_id)),
        client_secret=client_secret,
        authority=AzureAuthorityHosts.AZURE_GOVERNMENT,
    )

"""Reads the lakehouse's Silver and Gold Delta tables (plus the Databricks Sales table) over the OneLake API."""
import io
import json
import re
import sqlite3
import time
from urllib.parse import unquote
from uuid import UUID

import requests
from azure.identity import AzureAuthorityHosts, ClientSecretCredential

ONELAKE_SCOPE = "https://storage.azure.com/.default"
COMMIT_PATTERN = re.compile(r"(?P<version>\d{20})\.json$")
LAYER_PREFIXES = ("silver_", "gold_")


class ShortcutSource:
    def __init__(self, onelake_host, workspace, lakehouse, tenant_id, credential, session=None,
                 sales_table="dbo/sales", schema="dbo"):
        self.base = f"https://{onelake_host}"
        self.tables_root = f"{lakehouse}.Lakehouse/Tables/{schema}"
        self.sales_root = f"{lakehouse}.Lakehouse/Tables/{sales_table}"
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.tenant_id = str(UUID(tenant_id))
        self.credential = credential
        self.session = session or requests.Session()

    def _headers(self):
        return {"Authorization": f"Bearer {self.credential.get_token(ONELAKE_SCOPE).token}"}

    def read_bytes(self, name):
        response = self.session.get(f"{self.base}/{self.workspace}/{name}", headers=self._headers(),
                                    timeout=(10, 90), allow_redirects=False)
        response.raise_for_status()
        return response.content

    def _list(self, directory, directories=False):
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
            names += [p["name"] for p in response.json().get("paths", [])
                      if (str(p.get("isDirectory")).lower() == "true") == directories]
            token = response.headers.get("x-ms-continuation")
            if not token:
                return names

    def layer_tables(self):
        """Names of the Silver and Gold tables, discovered by listing the lakehouse schema folder."""
        names = (n.rstrip("/").rsplit("/", 1)[-1] for n in self._list(self.tables_root, directories=True))
        return sorted(n for n in names if n.startswith(LAYER_PREFIXES))

    def delta_files(self, root):
        """Active parquet files of a Delta table, found by replaying its _delta_log commits."""
        log_dir = f"{root}/_delta_log"
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
        return commits[-1][0], sorted(f"{root}/{path}" for path in active)

    def read_delta(self, root):
        """Returns (rows as dicts, delta version, parquet file paths)."""
        import pyarrow.parquet as pq

        version, files = self.delta_files(root)
        rows = []
        for name in files:
            rows += pq.read_table(io.BytesIO(self.read_bytes(name))).to_pylist()
        return rows, version, files


def _quote(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _create(conn, table, rows):
    columns = list(rows[0])
    conn.execute(f"CREATE TABLE {_quote(table)} ({', '.join(_quote(c) for c in columns)})")
    conn.executemany(f"INSERT INTO {_quote(table)} VALUES ({','.join('?' * len(columns))})",
                     [tuple(row[c] for c in columns) for row in rows])


def _layer(name):
    return name.split("_", 1)[0]


def load(source, include_sales=True):
    """Returns (sqlite connection, provenance). Delta tables are read through the lakehouse's OneLake path."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    tables, read_files, listing_error = [], [], None
    try:
        names = source.layer_tables()
    except (requests.RequestException, ValueError, KeyError, OSError) as exc:
        names, listing_error = [], type(exc).__name__
    for name in names:
        entry = {"name": name, "layer": _layer(name), "rows": 0, "version": None, "error": None}
        try:
            rows, version, files = source.read_delta(f"{source.tables_root}/{name}")
            if rows:
                _create(conn, name, rows)
            entry.update(rows=len(rows), version=version)
            read_files += [{"table": name, "path": path, "version": version} for path in files]
        except (requests.RequestException, ValueError, KeyError, OSError) as exc:
            entry["error"] = type(exc).__name__ + (f": {exc}" if isinstance(exc, ValueError) else "")
        tables.append(entry)
    conn.commit()
    sales = {"sales_path": f"{source.workspace}/{source.sales_root}", "sales_rows": 0, "sales_version": None,
             "sales_files": [], "sales_error": None}
    if include_sales:
        try:
            rows, version, sales_files = source.read_delta(source.sales_root)
            if rows:
                _create(conn, "Sales", rows)
                conn.commit()
            sales.update(sales_rows=len(rows), sales_version=version, sales_files=sales_files)
        except (requests.RequestException, ValueError, KeyError, OSError) as exc:
            sales["sales_error"] = type(exc).__name__ + (f": {exc}" if isinstance(exc, ValueError) else "")
    streams = conn.execute('SELECT COUNT(*), COUNT("AvgJitterMs") FROM silver_mediastreams').fetchone() \
        if any(t["name"] == "silver_mediastreams" and t["rows"] for t in tables) else (0, 0)
    provenance = {
        "shortcut_path": f"{source.workspace}/{source.tables_root}",
        "tables": tables,
        "tables_listed": len(names),
        "tables_read": sum(1 for t in tables if t["error"] is None),
        "tables_error": listing_error,
        "files": read_files,
        "stream_rows": streams[0],
        "stream_metric_coverage": (streams[1] / streams[0]) if streams[0] else 1.0,
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

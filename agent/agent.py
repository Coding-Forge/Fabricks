"""Gov fallback agent: Azure OpenAI function calling over the Fabric lakehouse SQL endpoint.

Required environment variables:
  AZURE_OPENAI_ENDPOINT   Azure OpenAI endpoint URL
  AZURE_OPENAI_API_KEY    key for the gpt-4o deployment (store in Key Vault, do not commit)
  FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET   service principal with workspace access
  FABRIC_SQL_SERVER, FABRIC_DATABASE   Fabric SQL analytics endpoint host and lakehouse name
Optional:
  AZURE_OPENAI_DEPLOYMENT, FABRIC_SQL_SCOPE
"""
import json
import os
import re
import struct
import sys

import pyodbc
from azure.identity import AzureAuthorityHosts, ClientSecretCredential
from openai import AzureOpenAI

OPENAI_ENDPOINT = os.environ["AZURE_OPENAI_ENDPOINT"]
DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
SQL_SERVER = os.environ["FABRIC_SQL_SERVER"]
DATABASE = os.environ["FABRIC_DATABASE"]
# Token audience for the Fabric Gov SQL endpoint; verify against current docs if auth fails.
SQL_SCOPE = os.getenv("FABRIC_SQL_SCOPE", "https://database.usgovcloudapi.net/.default")

SQL_COPT_SS_ACCESS_TOKEN = 1256
FORBIDDEN = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|merge|exec|execute|grant|revoke)\b", re.I)

SYSTEM_PROMPT = """You answer questions about data in a Fabric lakehouse by calling query_lakehouse.
Table: dbo.sales(id BIGINT, region BIGINT, amount DOUBLE) - sample sales rows.
Use T-SQL (SQL Server dialect). Only SELECT statements are allowed. Base answers strictly on query results."""

TOOLS = [{
    "type": "function",
    "function": {
        "name": "query_lakehouse",
        "description": "Run a single read-only T-SQL SELECT against the Fabric lakehouse SQL analytics endpoint.",
        "parameters": {
            "type": "object",
            "properties": {"sql": {"type": "string", "description": "A single T-SQL SELECT statement."}},
            "required": ["sql"],
        },
    },
}]

_credential = ClientSecretCredential(
    tenant_id=os.environ["FABRIC_TENANT_ID"],
    client_id=os.environ["FABRIC_CLIENT_ID"],
    client_secret=os.environ["FABRIC_CLIENT_SECRET"],
    authority=AzureAuthorityHosts.AZURE_GOVERNMENT,
)


def _connect():
    token = _credential.get_token(SQL_SCOPE).token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token)}s", len(token), token)
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={SQL_SERVER},1433;Database={DATABASE};Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


def query_lakehouse(sql: str, max_rows: int = 200) -> str:
    statement = sql.strip().rstrip(";")
    if not statement.lower().startswith("select") or ";" in statement or FORBIDDEN.search(statement):
        return "Rejected: only a single read-only SELECT statement is allowed."
    try:
        with _connect() as conn:
            cursor = conn.cursor()
            cursor.execute(statement)
            columns = [c[0] for c in cursor.description]
            rows = [dict(zip(columns, map(str, row))) for row in cursor.fetchmany(max_rows)]
        return json.dumps(rows)
    except pyodbc.Error as exc:
        return f"SQL error: {exc}"


def ask(client: AzureOpenAI, question: str) -> str:
    messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": question}]
    for _ in range(5):
        response = client.chat.completions.create(model=DEPLOYMENT, messages=messages, tools=TOOLS)
        message = response.choices[0].message
        if not message.tool_calls:
            return message.content
        messages.append(message)
        for call in message.tool_calls:
            sql = json.loads(call.function.arguments)["sql"]
            print(f"[tool] {sql}")
            messages.append({"role": "tool", "tool_call_id": call.id, "content": query_lakehouse(sql)})
    return "Stopped after too many tool calls."


if __name__ == "__main__":
    openai_client = AzureOpenAI(
        azure_endpoint=OPENAI_ENDPOINT,
        api_key=os.environ["AZURE_OPENAI_API_KEY"],
        api_version="2024-12-01-preview",
    )
    prompt = " ".join(sys.argv[1:]) or "What is the total amount by region?"
    print(ask(openai_client, prompt))

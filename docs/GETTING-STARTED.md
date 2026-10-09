# Getting Started

Chat with your Fabric data: ask plain-English questions and get answers from lakehouse data that Databricks wrote to ADLS Gen2 and Fabric exposes through shortcuts. Every answer shows the SQL that ran, the Delta files read, and a heuristic confidence score.

## What you need

- Python 3.10 or newer
- A Fabric workspace and lakehouse containing the `silver_*` and `gold_*` Delta tables and the `sales` shortcut (see the [README](../README.md), sections 4.3 and 12)
- A service principal (SPN) that is a **member of the Fabric workspace**, and the tenant setting that lets service principals use Fabric APIs
- An Azure OpenAI deployment (this POC uses `gpt-4o`, Data Zone Standard, in USGov Virginia)
- Only for `agent/agent.py`: ODBC Driver 18 for SQL Server

## Configure

Copy the template and fill it in. Take secrets from Key Vault.

```bash
cp .env.example .env
set -a; source .env; set +a
```

| Variable | Required | Description |
|----------|----------|-------------|
| `FABRIC_TENANT_ID` | Yes | Entra tenant ID |
| `FABRIC_CLIENT_ID` | Yes | SPN application (client) ID |
| `FABRIC_CLIENT_SECRET` | Yes | SPN secret (`my_secret` is also accepted) |
| `AZURE_OPENAI_ENDPOINT` | Yes | For Gov, `https://<resource>.openai.azure.us/` |
| `AZURE_OPENAI_API_KEY` | Yes | Key for the deployment |
| `AZURE_OPENAI_DEPLOYMENT` | No | Default `gpt-4o` |
| `FABRIC_WORKSPACE` | No | Default `CQD` |
| `FABRIC_LAKEHOUSE` | No | Default `cqd_lh` |
| `LAKEHOUSE_SCHEMA` | No | Schema folder under `Tables/` that holds the `silver_*` and `gold_*` tables, default `dbo` |
| `SALES_TABLE` | No | Delta table path under `Tables/`, default `dbo/sales` (use `sales` for a lakehouse without schemas) |
| `ONELAKE_HOST` | No | Default `high-onelake.dfs.fabric.microsoft.us` |
| `CHAT_PORT` | No | Default `8000` |
| `CHAT_REFRESH_SECONDS` | No | How long loaded data is reused, default `300` |

## Run

```bash
python -m venv .venv
.venv/bin/pip install -r chat/requirements.txt
.venv/bin/python -m chat.server --check   # no model needed
.venv/bin/python -m chat.server           # http://127.0.0.1:8000
```

A healthy `--check` shows `sales_rows` greater than 0 and `"sales_error": null`, and lists each `silver_*` and `gold_*` table with its row count and Delta version. Open http://127.0.0.1:8000 and click an example question or type your own. **Refresh** reloads the data from the shortcuts, so new Databricks writes appear without restarting.

## Example questions

- What is the total sales amount by region?
- How many sales records do we have?
- Which platform has the highest average jitter?
- How many calls are there by type?

## Reading an answer

- **SQL:** the exact read-only `SELECT` statements the model ran. Check them when a number matters.
- **Delta files:** the parquet files read from the lakehouse for that answer.
- **Confidence score:** a heuristic made from query success, whether the numbers in the answer appear in the query results, and how complete the source data was. It is **not a measured accuracy**. Low scores usually mean no rows came back or the answer cited numbers the query did not return.
- The sales table is synthetic sample data (`amount` is between 0 and 1, no currency) and is unrelated to the call tables. Call questions are answered from the Silver and Gold layers, not Bronze.

## Use your own Delta table

Point `SALES_TABLE` at another Delta table that has a shortcut under `Tables/`. The app reads it by replaying `_delta_log` and loading the active parquet files into memory, so it suits small tables. Limitations:

- Tables whose log has been checkpointed (after roughly 10 commits) are reported as an error, not read partially.
- Tables using deletion vectors are not supported.
- The loaded table is named `Sales` and the model is told about the sales columns only; adjust `system_prompt` in `chat/engine.py` for a different schema.

## Troubleshooting

| Symptom | Cause and fix |
|---------|---------------|
| `KeyError: 'AZURE_OPENAI_ENDPOINT'` | Variable not exported. Run `set -a; source .env; set +a` in the same shell. |
| `sales_error` mentions 403 | The SPN is not a workspace member, or the service principal tenant setting is off. |
| `sales_error` mentions 404 or `sales_rows` is 0 | Wrong `SALES_TABLE`, or the shortcut sits under **Files**. A Tables shortcut must point at the Delta table folder (the one containing `_delta_log`). A parent folder lands in *Unidentified*. |
| The source line says fewer tables read than listed | A `silver_*` or `gold_*` table failed to load. Run `--check` and read that table's `error`; checkpointed logs and deletion vectors are not supported. |
| `sales_error` mentions "checkpointed" | The Delta log was compacted. Use the SQL analytics endpoint approach in `agent/agent.py`. |
| Databricks cluster fails with `DatabricksWorkerArm64` not found | Arm node types are unavailable in Gov. Use an x86 size such as `Standard_DS3_v2`. |
| Unity Catalog options missing in Databricks | Not available in this Gov environment. Use SPN OAuth with a Key Vault-backed secret scope (README section 4.2). |
| Page shows old data | Click **Refresh**, or wait `CHAT_REFRESH_SECONDS`. |
| Fabric Data agent or Copilot missing | Not available in the Gov tenant (pending confirmation). The app does not need them. |

## Security notes

- Keep secrets in Key Vault and in `.env`; `.env` is git-ignored. Never commit or print them.
- The server binds to `127.0.0.1` only and runs read-only SQL against in-memory copies of the data.
- The POC uses public network access. Production needs private networking (README sections 7 and 8).

## Tests

```bash
.venv/bin/python -m unittest tests.test_chat
```

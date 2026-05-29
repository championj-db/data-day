# Databricks notebook source
# MAGIC %md
# MAGIC # APRA Super Data Day — Environment Setup
# MAGIC
# MAGIC Run this **once** on **Serverless** to build everything you need for the hands-on session.
# MAGIC
# MAGIC **What it does (≈2–4 min):**
# MAGIC 1. Creates the schema **`workspace.data_day`**
# MAGIC 2. Loads 3 APRA superannuation tables from the bundled Parquet files in `../data/`
# MAGIC 3. Adds table + column descriptions (great for Genie)
# MAGIC 4. Creates the metric view **`mv_super_fund_allocation`** (the governed semantic layer for Genie)
# MAGIC 5. Creates & publishes the **APRA Super Data Day** AI/BI dashboard
# MAGIC
# MAGIC **Your exercise afterwards:** build and configure a **Genie space** on `workspace.data_day`.
# MAGIC
# MAGIC > Data source: [APRA Quarterly Superannuation Statistics](https://www.apra.gov.au/quarterly-superannuation-statistics) (public). No Excel parsing — the tables ship as Parquet.

# COMMAND ----------
# MAGIC %md ## 1 · Configuration

# COMMAND ----------

import os

# Free Edition's default catalog is `workspace`. Use it if present; otherwise fall back
# to whatever this workspace's current catalog is.
_catalogs = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
CATALOG = "workspace" if "workspace" in _catalogs else spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "data_day"
FQ = f"{CATALOG}.{SCHEMA}"
DASHBOARD_NAME = "APRA Super Data Day"

# Locate the bundled data/ directory relative to this notebook (works in a Git folder).
def _find_data_dir():
    candidates = []
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        candidates.append("/Workspace" + os.path.dirname(os.path.dirname(nb)) + "/data")
    except Exception:
        pass
    candidates.append(os.path.join(os.getcwd(), "data"))
    candidates.append(os.path.join(os.path.dirname(os.getcwd()), "data"))
    for c in candidates:
        if os.path.isdir(c):
            return c
    raise FileNotFoundError(f"Could not locate the data/ folder. Tried: {candidates}")

DATA_DIR = _find_data_dir()
print(f"Target schema : {FQ}")
print(f"Data directory: {DATA_DIR}")
print("Files         :", sorted(f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')))

# COMMAND ----------
# MAGIC %md ## 2 · Create the schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT 'APRA superannuation data for Data Day hands-on labs.'")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Using {FQ}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Load the tables from Parquet
# MAGIC
# MAGIC On serverless, Spark cannot read files directly from a `/Workspace` Git-folder path, so we
# MAGIC read each small Parquet with **pandas** (on the driver) and write it as a managed Delta table.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# table -> columns that should be typed as DATE (stored as text in Parquet)
DATE_COLS = {
    "super_fund_membership": ["period"],
    "super_fund_asset_allocation": ["period"],
    "super_performance": [],
}

def load_table(name):
    pdf = pd.read_parquet(f"{DATA_DIR}/{name}.parquet")
    # normalise pandas NA -> None so Spark infers nullable columns cleanly
    pdf = pdf.astype(object).where(pd.notna(pdf), None)
    sdf = spark.createDataFrame(pdf)
    for c in DATE_COLS.get(name, []):
        sdf = sdf.withColumn(c, col(c).cast("date"))
    (sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.{name}"))
    n = spark.table(f"{FQ}.{name}").count()
    print(f"  {name}: {n} rows")

print("Loading tables...")
for t in ["super_fund_membership", "super_fund_asset_allocation", "super_performance"]:
    load_table(t)
print("Done.")

# COMMAND ----------
# MAGIC %md ## 4 · Table & column descriptions
# MAGIC Good comments make the Genie space dramatically better — Genie reads them as context.

# COMMAND ----------

APRA_SUPER = "https://www.apra.gov.au/quarterly-superannuation-statistics"

TABLE_COMMENTS = {
    "super_fund_membership":
        f"Fund-level membership profile for APRA-regulated superannuation funds (member accounts, "
        f"balances, demographics). Dec 2025. Source: APRA Quarterly Superannuation Fund Statistics "
        f"Table 1. {APRA_SUPER}",
    "super_fund_asset_allocation":
        f"Fund-level asset allocation across cash, fixed income, equity, property, infrastructure and "
        f"alternatives. Values in $M AUD. Dec 2025. Source: APRA Quarterly Superannuation Fund Statistics "
        f"Table 4. {APRA_SUPER}",
    "super_performance":
        f"Quarterly superannuation INDUSTRY asset allocation by asset class (long format: quarter, "
        f"asset_class, value_m). Dec 2004 to Dec 2025. Values in $M AUD. Source: APRA Quarterly "
        f"Superannuation Performance Statistics Table 1d. {APRA_SUPER}",
}

COLUMN_COMMENTS = {
    "super_fund_membership": {
        "period": "Reporting period (quarter end date).",
        "fund_name": "Name of the superannuation fund (RSE).",
        "abn": "Australian Business Number of the fund.",
        "rse_regulatory_classification": "Regulatory classification — 'public offer' or 'non-public offer'.",
        "fund_type": "Type of fund (e.g. Retail, Industry, Public Sector, Corporate).",
        "rse_licensee_profit_status": "Licensee profit status — 'For profit status' or 'Not for profit status'.",
        "total_member_accounts": "Total number of member accounts in the fund.",
        "median_member_age": "Median age of fund members.",
    },
    "super_fund_asset_allocation": {
        "period": "Reporting period (quarter end date).",
        "fund_name": "Name of the superannuation fund (RSE).",
        "abn": "Australian Business Number of the fund.",
        "rse_regulatory_classification": "Regulatory classification — 'public offer' or 'non-public offer'.",
        "rse_licensee_profit_status": "Licensee profit status — 'For profit status' or 'Not for profit status'.",
        "cash_m": "Cash holdings ($M AUD).",
        "fixed_income_m": "Total fixed income holdings ($M AUD).",
        "equity_m": "Total equity holdings ($M AUD).",
        "property_m": "Total property holdings ($M AUD).",
        "infrastructure_m": "Total infrastructure holdings ($M AUD).",
        "alternatives_m": "Alternatives holdings ($M AUD).",
        "private_debt_m": "Private debt holdings ($M AUD).",
        "au_unlisted_property_m": "Australian unlisted (illiquid) property ($M AUD).",
        "intl_unlisted_property_m": "International unlisted (illiquid) property ($M AUD).",
        "au_unlisted_infrastructure_m": "Australian unlisted (illiquid) infrastructure ($M AUD).",
        "intl_unlisted_infrastructure_m": "International unlisted (illiquid) infrastructure ($M AUD).",
        "total_fund_investments_m": "Total fund investments ($M AUD) — the portfolio denominator.",
    },
    "super_performance": {
        "quarter": "Reporting quarter as text, e.g. 'Dec 2025'. Use try_to_date(quarter,'MMM yyyy') for time series.",
        "asset_class": "Industry asset class. Top-level classes (Equity, Fixed income, Cash, Property, Infrastructure, Other, Commodities) sum to 'Total investments'. The table also contains proportion rows (<1) and sub-items.",
        "value_m": "Value in $M AUD (dollar rows) — proportion rows are < 1.",
    },
}

for tname, tcomment in TABLE_COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {FQ}.{tname} IS {repr(tcomment)}")
    for cname, ccomment in COLUMN_COMMENTS.get(tname, {}).items():
        spark.sql(f"ALTER TABLE {FQ}.{tname} ALTER COLUMN {cname} COMMENT {repr(ccomment)}")
    print(f"  commented {tname}")
print("Descriptions applied.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 · Metric view `mv_super_fund_allocation`
# MAGIC A Unity Catalog **metric view** — governed, reusable measures (illiquid %, allocation %, AUM)
# MAGIC that Genie can query with `MEASURE(...)`. Metric views are GA on current Databricks serverless.

# COMMAND ----------

METRIC_VIEW_YAML = f"""
  version: 1.1
  comment: >
    Superannuation fund asset allocation metrics including illiquid asset exposure,
    portfolio composition, and peer comparisons. Fund-level data from APRA Quarterly
    Fund-Level Statistics Table 4. Dec 2025.
    Source: https://www.apra.gov.au/quarterly-superannuation-statistics
  source: {FQ}.super_fund_asset_allocation
  filter: total_fund_investments_m > 0
  dimensions:
    - name: Fund Name
      expr: fund_name
    - name: ABN
      expr: CAST(abn AS STRING)
    - name: Classification
      expr: rse_regulatory_classification
    - name: Fund Type
      expr: fund_type
    - name: Membership Base
      expr: rse_membership_base
    - name: Licensee
      expr: rse_licensee
    - name: Profit Status
      expr: rse_licensee_profit_status
    - name: Period
      expr: period
  measures:
    - name: Fund Count
      expr: COUNT(DISTINCT fund_name)
    - name: Total Investments Bn
      expr: ROUND(SUM(total_fund_investments_m) / 1000, 1)
    - name: Avg Equity Pct
      expr: ROUND(AVG(equity_m / NULLIF(total_fund_investments_m, 0) * 100), 1)
    - name: Avg Fixed Income Pct
      expr: ROUND(AVG(fixed_income_m / NULLIF(total_fund_investments_m, 0) * 100), 1)
    - name: Avg Property Pct
      expr: ROUND(AVG(property_m / NULLIF(total_fund_investments_m, 0) * 100), 1)
    - name: Avg Infrastructure Pct
      expr: ROUND(AVG(infrastructure_m / NULLIF(total_fund_investments_m, 0) * 100), 1)
    - name: Avg Alternatives Pct
      expr: ROUND(AVG(alternatives_m / NULLIF(total_fund_investments_m, 0) * 100), 1)
    - name: Avg Cash Pct
      expr: ROUND(AVG(cash_m / NULLIF(total_fund_investments_m, 0) * 100), 1)
    - name: Avg Illiquid Pct
      expr: >
        ROUND(AVG(
          (COALESCE(au_unlisted_property_m, 0) + COALESCE(intl_unlisted_property_m, 0) +
           COALESCE(au_unlisted_infrastructure_m, 0) + COALESCE(intl_unlisted_infrastructure_m, 0) +
           COALESCE(alternatives_m, 0) + COALESCE(private_debt_m, 0))
          / NULLIF(total_fund_investments_m, 0) * 100
        ), 1)
    - name: Max Illiquid Pct
      expr: >
        ROUND(MAX(
          (COALESCE(au_unlisted_property_m, 0) + COALESCE(intl_unlisted_property_m, 0) +
           COALESCE(au_unlisted_infrastructure_m, 0) + COALESCE(intl_unlisted_infrastructure_m, 0) +
           COALESCE(alternatives_m, 0) + COALESCE(private_debt_m, 0))
          / NULLIF(total_fund_investments_m, 0) * 100
        ), 1)
    - name: Total Private Debt Bn
      expr: ROUND(SUM(COALESCE(private_debt_m, 0)) / 1000, 1)
"""

spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.mv_super_fund_allocation
WITH METRICS LANGUAGE YAML AS $${METRIC_VIEW_YAML}$$""")
print("✓ Metric view created. Try: SELECT `Classification`, MEASURE(`Avg Illiquid Pct`) "
      f"FROM {FQ}.mv_super_fund_allocation GROUP BY ALL")

# COMMAND ----------
# MAGIC %md ## 6 · Verify

# COMMAND ----------

print("Row counts:")
for t in ["super_fund_membership", "super_fund_asset_allocation", "super_performance"]:
    print(f"  {t}: {spark.table(f'{FQ}.{t}').count()}")

print("\nMetric view — allocation by classification:")
display(spark.sql(f"""
    SELECT `Classification`, MEASURE(`Fund Count`) AS funds,
           MEASURE(`Avg Equity Pct`) AS avg_equity, MEASURE(`Avg Illiquid Pct`) AS avg_illiquid
    FROM {FQ}.mv_super_fund_allocation GROUP BY ALL"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7 · Create & publish the AI/BI dashboard
# MAGIC Reads the committed `dashboard/super_data_day.lvdash.json` and publishes it against your
# MAGIC serverless SQL warehouse. Re-running updates the same dashboard (no duplicates).

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

# Load the dashboard definition committed in the repo (dashboard/super_data_day.lvdash.json).
DASH_FILE = os.path.join(os.path.dirname(DATA_DIR), "dashboard", "super_data_day.lvdash.json")
with open(DASH_FILE) as f:
    serialized = f.read()
# The committed dashboard targets `workspace.data_day`; adapt only if this workspace differs.
if FQ != "workspace.data_day":
    serialized = serialized.replace("workspace.data_day", FQ)
print(f"Loaded dashboard definition from {DASH_FILE}")

w = WorkspaceClient()

# Discover a serverless SQL warehouse (Free Edition ships one).
warehouses = list(w.warehouses.list())
serverless = [x for x in warehouses if getattr(x, "enable_serverless_compute", False)]
if not warehouses:
    raise RuntimeError("No SQL warehouse found. Open SQL → SQL Warehouses and start one, then re-run this cell.")
warehouse_id = (serverless or warehouses)[0].id
print(f"Using warehouse: {(serverless or warehouses)[0].name} ({warehouse_id})")

# Idempotent: update the dashboard if it already exists, else create it.
existing = None
for d in (w.api_client.do("GET", "/api/2.0/lakeview/dashboards").get("dashboards", []) or []):
    if d.get("display_name") == DASHBOARD_NAME and d.get("lifecycle_state") != "TRASHED":
        existing = d
        break

if existing:
    dash_id = existing["dashboard_id"]
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dash_id}",
                    body={"display_name": DASHBOARD_NAME, "warehouse_id": warehouse_id,
                          "serialized_dashboard": serialized})
    print(f"Updated existing dashboard {dash_id}")
else:
    created = w.api_client.do("POST", "/api/2.0/lakeview/dashboards",
                              body={"display_name": DASHBOARD_NAME, "warehouse_id": warehouse_id,
                                    "serialized_dashboard": serialized})
    dash_id = created["dashboard_id"]
    print(f"Created dashboard {dash_id}")

w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dash_id}/published",
                body={"warehouse_id": warehouse_id, "embed_credentials": True})

host = w.config.host.rstrip("/")
print(f"\n✓ Published: {host}/sql/dashboardsv3/{dash_id}/published")
print(f"  Draft:     {host}/sql/dashboardsv3/{dash_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Done — now build your Genie space
# MAGIC
# MAGIC You now have, in **`workspace.data_day`**:
# MAGIC - 3 described tables · 1 metric view (`mv_super_fund_allocation`) · a published dashboard.
# MAGIC
# MAGIC *Optional:* run `sql/v_super_illiquidity_peer.sql` to add a per-fund illiquid-vs-peers view — a
# MAGIC handy extra asset when you build your Genie space.
# MAGIC
# MAGIC **Your turn:** Go to **Genie → New**, scope it to the `workspace.data_day` schema, and try:
# MAGIC - *"Which funds have the highest illiquid asset exposure compared with their peers?"*
# MAGIC - *"What is the average equity allocation for public offer vs non-public offer funds?"*
# MAGIC - *"How has total industry superannuation AUM changed over time?"*
# MAGIC
# MAGIC Then improve it: add instructions, sample questions, and certified SQL. The table/column
# MAGIC comments and the metric view give Genie a strong head start.

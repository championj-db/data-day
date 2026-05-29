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
# MAGIC 4. Creates the metric view **`mv_super_fund_allocation`** and the analytical view **`v_super_illiquidity_peer`**
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
# MAGIC that Genie and dashboards can query with `MEASURE(...)`. Requires a recent serverless runtime;
# MAGIC if unavailable, the cell falls back to a plain view exposing the same per-fund calculations.

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

try:
    spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.mv_super_fund_allocation
WITH METRICS LANGUAGE YAML AS $${METRIC_VIEW_YAML}$$""")
    print("✓ Metric view created. Try: SELECT `Classification`, MEASURE(`Avg Illiquid Pct`) "
          f"FROM {FQ}.mv_super_fund_allocation GROUP BY ALL")
except Exception as e:
    print(f"⚠ Metric view not supported on this runtime ({str(e)[:120]}...). Creating a plain fallback view instead.")
    spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.mv_super_fund_allocation AS
        SELECT fund_name, CAST(abn AS STRING) AS abn, rse_regulatory_classification AS classification,
               fund_type, rse_licensee_profit_status AS profit_status, period,
               total_fund_investments_m,
               ROUND(equity_m / NULLIF(total_fund_investments_m,0) * 100, 1) AS equity_pct,
               ROUND(fixed_income_m / NULLIF(total_fund_investments_m,0) * 100, 1) AS fixed_income_pct,
               ROUND(cash_m / NULLIF(total_fund_investments_m,0) * 100, 1) AS cash_pct,
               ROUND((COALESCE(au_unlisted_property_m,0)+COALESCE(intl_unlisted_property_m,0)+
                      COALESCE(au_unlisted_infrastructure_m,0)+COALESCE(intl_unlisted_infrastructure_m,0)+
                      COALESCE(alternatives_m,0)+COALESCE(private_debt_m,0))
                     / NULLIF(total_fund_investments_m,0) * 100, 1) AS illiquid_pct
        FROM {FQ}.super_fund_asset_allocation WHERE total_fund_investments_m > 0""")
    spark.sql(f"COMMENT ON TABLE {FQ}.mv_super_fund_allocation IS "
              f"'Per-fund super allocation percentages (fallback plain view — metric view unsupported on this runtime).'")
    print("✓ Fallback view created.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 · Analytical view `v_super_illiquidity_peer`
# MAGIC Fund illiquid-asset % with peer-segment averages. Powers the dashboard and answers the
# MAGIC question: *"Which funds have the highest illiquid exposure vs peers?"*

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.v_super_illiquidity_peer AS
WITH fund_illiquid AS (
    SELECT
        fund_name, abn, rse_regulatory_classification, fund_type, rse_membership_base,
        rse_licensee, rse_licensee_profit_status, period,
        cash_m, fixed_income_m, equity_m, property_m, infrastructure_m, alternatives_m,
        private_debt_m, total_fund_investments_m,
        COALESCE(au_unlisted_property_m, 0) + COALESCE(intl_unlisted_property_m, 0) +
        COALESCE(au_unlisted_infrastructure_m, 0) + COALESCE(intl_unlisted_infrastructure_m, 0) +
        COALESCE(alternatives_m, 0) + COALESCE(private_debt_m, 0) AS illiquid_assets_m,
        CASE WHEN total_fund_investments_m > 0 THEN
            (COALESCE(au_unlisted_property_m, 0) + COALESCE(intl_unlisted_property_m, 0) +
             COALESCE(au_unlisted_infrastructure_m, 0) + COALESCE(intl_unlisted_infrastructure_m, 0) +
             COALESCE(alternatives_m, 0) + COALESCE(private_debt_m, 0))
            / total_fund_investments_m * 100
        ELSE NULL END AS illiquid_pct
    FROM {FQ}.super_fund_asset_allocation
    WHERE total_fund_investments_m > 0
),
segment_avg AS (
    SELECT rse_regulatory_classification,
        COUNT(*) AS funds_in_segment,
        AVG(illiquid_pct) AS segment_avg_illiquid_pct,
        STDDEV(illiquid_pct) AS segment_stddev_illiquid_pct,
        PERCENTILE(illiquid_pct, 0.5) AS segment_median_illiquid_pct
    FROM fund_illiquid GROUP BY rse_regulatory_classification
)
SELECT f.*,
    s.funds_in_segment, s.segment_avg_illiquid_pct, s.segment_stddev_illiquid_pct,
    s.segment_median_illiquid_pct,
    f.illiquid_pct - s.segment_avg_illiquid_pct AS vs_peer_avg_pp,
    CASE
        WHEN f.illiquid_pct > s.segment_avg_illiquid_pct + s.segment_stddev_illiquid_pct THEN 'Above average'
        WHEN f.illiquid_pct < s.segment_avg_illiquid_pct - s.segment_stddev_illiquid_pct THEN 'Below average'
        ELSE 'Within range'
    END AS peer_comparison
FROM fund_illiquid f
JOIN segment_avg s ON f.rse_regulatory_classification = s.rse_regulatory_classification
""")
spark.sql(f"COMMENT ON TABLE {FQ}.v_super_illiquidity_peer IS "
          f"'Fund-level illiquid asset exposure (% of portfolio) with peer-segment averages. "
          f"Illiquid = unlisted property + unlisted infrastructure + alternatives + private debt. "
          f"Source: APRA Quarterly Superannuation Fund Statistics Table 4.'")
print("✓ Analytical view created.")

# COMMAND ----------
# MAGIC %md ## 7 · Verify

# COMMAND ----------

print("Row counts:")
for t in ["super_fund_membership", "super_fund_asset_allocation", "super_performance"]:
    print(f"  {t}: {spark.table(f'{FQ}.{t}').count()}")

print("\nTop 5 funds by illiquid exposure vs peers:")
display(spark.sql(f"""
    SELECT fund_name, rse_regulatory_classification AS classification,
           ROUND(illiquid_pct,1) AS illiquid_pct, ROUND(segment_avg_illiquid_pct,1) AS peer_avg_pct
    FROM {FQ}.v_super_illiquidity_peer ORDER BY illiquid_pct DESC LIMIT 5"""))

# Metric view sanity check (works only if the metric view form was created)
try:
    display(spark.sql(f"""
        SELECT `Classification`, MEASURE(`Fund Count`) AS funds, MEASURE(`Avg Illiquid Pct`) AS avg_illiquid
        FROM {FQ}.mv_super_fund_allocation GROUP BY ALL"""))
except Exception as e:
    print(f"(Metric-view MEASURE() query skipped — fallback view in use: {str(e)[:80]})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8 · Create & publish the AI/BI dashboard
# MAGIC Builds the **APRA Super Data Day** dashboard (Fund Risk & Allocation + Industry Trends) and
# MAGIC publishes it against your serverless SQL warehouse. Re-running updates the same dashboard.

# COMMAND ----------

import hashlib
import json
from databricks.sdk import WorkspaceClient

# Regulatory blue/teal palette
BLUE, TEAL, GREEN, AMBER, RED, SLATE = "#1B3A5C", "#00838F", "#059669", "#F59E0B", "#DC2626", "#64748B"
SEG = [BLUE, TEAL, GREEN, AMBER, SLATE]


def build_dashboard(fq: str) -> dict:
    """Returns the serialized AI/BI dashboard. Reads ONLY v_super_illiquidity_peer and
    super_performance, so it renders regardless of metric-view runtime support."""
    def uid(name):
        return hashlib.md5(name.encode()).hexdigest()[:8]

    datasets, layout1, layout2 = [], [], []

    def ds(key, display, sql):
        datasets.append({"name": uid(key), "displayName": display, "queryLines": [sql]})
        return uid(key)

    def text(key, md, pos):
        return {"widget": {"name": uid(key), "textbox_spec": md}, "position": pos}

    def counter(key, dsid, field, title, pos):
        return {"widget": {"name": uid(key), "queries": [{"name": "main_query", "query": {
            "datasetName": dsid, "fields": [{"name": field, "expression": f"SUM(`{field}`)"}],
            "disaggregated": True}}],
            "spec": {"version": 2, "widgetType": "counter",
                     "encodings": {"value": {"fieldName": field, "displayName": title}},
                     "frame": {"showTitle": True, "title": title}}}, "position": pos}

    def bar(key, dsid, xf, yf, title, pos, xname, yname, colors=None):
        return {"widget": {"name": uid(key), "queries": [{"name": "main_query", "query": {
            "datasetName": dsid, "fields": [{"name": xf, "expression": f"`{xf}`"},
                       {"name": yf, "expression": f"SUM(`{yf}`)"}], "disaggregated": False}}],
            "spec": {"version": 3, "widgetType": "bar",
                     "encodings": {"x": {"fieldName": xf, "scale": {"type": "categorical", "sort": {"by": "y-reversed"}}, "displayName": xname},
                                   "y": {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": yname},
                                   "label": {"show": True}},
                     "frame": {"showTitle": True, "title": title}, "mark": {"colors": colors or SEG}}}, "position": pos}

    def pie(key, dsid, angle, cat, title, pos):
        return {"widget": {"name": uid(key), "queries": [{"name": "main_query", "query": {
            "datasetName": dsid, "fields": [{"name": angle, "expression": f"SUM(`{angle}`)"},
                       {"name": cat, "expression": f"`{cat}`"}], "disaggregated": False}}],
            "spec": {"version": 3, "widgetType": "pie",
                     "encodings": {"angle": {"fieldName": angle, "scale": {"type": "quantitative"}, "displayName": title},
                                   "color": {"fieldName": cat, "scale": {"type": "categorical"}, "displayName": cat}},
                     "frame": {"showTitle": True, "title": title}, "mark": {"colors": SEG}}}, "position": pos}

    def scatter(key, dsid, xf, yf, cf, title, pos, xname, yname):
        return {"widget": {"name": uid(key), "queries": [{"name": "main_query", "query": {
            "datasetName": dsid, "fields": [{"name": xf, "expression": f"`{xf}`"},
                       {"name": yf, "expression": f"`{yf}`"}, {"name": cf, "expression": f"`{cf}`"}],
            "disaggregated": True}}],
            "spec": {"version": 3, "widgetType": "scatter",
                     "encodings": {"x": {"fieldName": xf, "scale": {"type": "quantitative"}, "displayName": xname},
                                   "y": {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": yname},
                                   "color": {"fieldName": cf, "scale": {"type": "categorical"}, "displayName": "Classification"}},
                     "frame": {"showTitle": True, "title": title}, "mark": {"colors": SEG}}}, "position": pos}

    def line(key, dsid, xf, yf, title, pos, xname, yname):
        return {"widget": {"name": uid(key), "queries": [{"name": "main_query", "query": {
            "datasetName": dsid, "fields": [{"name": xf, "expression": f"`{xf}`"}, {"name": yf, "expression": f"`{yf}`"}],
            "disaggregated": True}}],
            "spec": {"version": 3, "widgetType": "line",
                     "encodings": {"x": {"fieldName": xf, "scale": {"type": "temporal"}, "displayName": xname},
                                   "y": {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": yname}},
                     "frame": {"showTitle": True, "title": title}, "mark": {"colors": [BLUE]}}}, "position": pos}

    def area(key, dsid, xf, yf, cf, title, pos, xname, yname):
        return {"widget": {"name": uid(key), "queries": [{"name": "main_query", "query": {
            "datasetName": dsid, "fields": [{"name": xf, "expression": f"`{xf}`"}, {"name": yf, "expression": f"`{yf}`"},
                       {"name": cf, "expression": f"`{cf}`"}], "disaggregated": True}}],
            "spec": {"version": 3, "widgetType": "area",
                     "encodings": {"x": {"fieldName": xf, "scale": {"type": "temporal"}, "displayName": xname},
                                   "y": {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": yname},
                                   "color": {"fieldName": cf, "scale": {"type": "categorical"}, "displayName": "Asset class"}},
                     "frame": {"showTitle": True, "title": title},
                     "mark": {"colors": [BLUE, TEAL, GREEN, AMBER, RED, SLATE, "#7C3AED"]}}}, "position": pos}

    def table(key, dsid, cols, title, pos):
        fields = [{"name": c[0], "expression": f"`{c[0]}`"} for c in cols]
        colenc = []
        for i, c in enumerate(cols):
            e = {"fieldName": c[0], "type": c[1], "displayAs": ("number" if c[1] == "float" else "string"),
                 "title": c[2], "displayName": c[2], "order": 100000 + i}
            if len(c) > 3 and c[3]:
                e["numberFormat"] = c[3]
            if c[1] == "float":
                e["alignContent"] = "right"
            colenc.append(e)
        return {"widget": {"name": uid(key), "queries": [{"name": "main_query", "query": {
            "datasetName": dsid, "fields": fields, "disaggregated": True}}],
            "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": colenc},
                     "frame": {"showTitle": True, "title": title}}}, "position": pos}

    V = f"{fq}.v_super_illiquidity_peer"
    P = f"{fq}.super_performance"

    d_summary = ds("summary", "Super Summary",
        f"SELECT COUNT(DISTINCT fund_name) AS fund_count, ROUND(SUM(total_fund_investments_m)/1000,1) AS total_inv_bn, "
        f"ROUND(AVG(illiquid_pct),1) AS avg_illiquid_pct FROM {V}")
    d_class = ds("byclass", "Allocation by Classification",
        f"SELECT rse_regulatory_classification AS classification, "
        f"ROUND(AVG(equity_m/NULLIF(total_fund_investments_m,0)*100),1) AS avg_equity_pct, "
        f"ROUND(AVG(illiquid_pct),1) AS avg_illiquid_pct, "
        f"ROUND(AVG(fixed_income_m/NULLIF(total_fund_investments_m,0)*100),1) AS avg_fi_pct, "
        f"ROUND(AVG(cash_m/NULLIF(total_fund_investments_m,0)*100),1) AS avg_cash_pct "
        f"FROM {V} GROUP BY rse_regulatory_classification")
    d_profit = ds("byprofit", "AUM by Profit Status",
        f"SELECT rse_licensee_profit_status AS profit_status, ROUND(SUM(total_fund_investments_m)/1000,1) AS total_inv_bn "
        f"FROM {V} WHERE rse_licensee_profit_status IS NOT NULL GROUP BY rse_licensee_profit_status")
    d_scatter = ds("scatter", "Illiquidity vs Size",
        f"SELECT fund_name, rse_regulatory_classification AS classification, ROUND(illiquid_pct,1) AS illiquid_pct, "
        f"ROUND(total_fund_investments_m,0) AS total_investments_m FROM {V} WHERE total_fund_investments_m>0")
    d_top = ds("topfunds", "Top Illiquid Funds",
        f"SELECT fund_name, rse_regulatory_classification AS classification, ROUND(illiquid_pct,1) AS illiquid_pct, "
        f"ROUND(segment_avg_illiquid_pct,1) AS peer_avg_pct, ROUND(vs_peer_avg_pp,1) AS vs_peer_pp, "
        f"ROUND(total_fund_investments_m,0) AS total_m, peer_comparison FROM {V} ORDER BY illiquid_pct DESC LIMIT 15")
    d_total = ds("totalaum", "Total Industry Investments",
        f"SELECT try_to_date(quarter,'MMM yyyy') AS quarter_date, ROUND(SUM(value_m)/1000,0) AS total_bn "
        f"FROM {P} WHERE asset_class='Total investments' AND value_m>10 "
        f"AND try_to_date(quarter,'MMM yyyy') IS NOT NULL GROUP BY 1 ORDER BY 1")
    d_mix = ds("allocmix", "Asset Allocation Mix Over Time",
        f"SELECT try_to_date(quarter,'MMM yyyy') AS quarter_date, asset_class, ROUND(SUM(value_m)/1000,1) AS value_bn "
        f"FROM {P} WHERE asset_class IN ('Cash','Fixed income','Equity','Property','Infrastructure','Other','Commodities') "
        f"AND value_m>10 AND try_to_date(quarter,'MMM yyyy') IS NOT NULL GROUP BY 1,2 ORDER BY 1")

    layout1 += [
        text("p1head",
             "# APRA Superannuation Fund Risk & Allocation\n"
             "Fund-level asset allocation and illiquid-asset exposure for APRA-regulated superannuation funds, "
             "compared against peer averages by regulatory classification. Data: APRA Quarterly Superannuation "
             "Fund Statistics (Dec 2025). *Built for Data Day on Databricks Free Edition.*",
             {"x": 0, "y": 0, "width": 6, "height": 2}),
        counter("c_funds", d_summary, "fund_count", "Super Funds", {"x": 0, "y": 2, "width": 2, "height": 2}),
        counter("c_aum", d_summary, "total_inv_bn", "Total Investments ($B)", {"x": 2, "y": 2, "width": 2, "height": 2}),
        counter("c_illiq", d_summary, "avg_illiquid_pct", "Avg Illiquid (%)", {"x": 4, "y": 2, "width": 2, "height": 2}),
        bar("b_equity", d_class, "classification", "avg_equity_pct", "Avg Equity Allocation by Classification (%)",
            {"x": 0, "y": 4, "width": 3, "height": 5}, "Classification", "Avg Equity %"),
        bar("b_illiq", d_class, "classification", "avg_illiquid_pct", "Avg Illiquid Exposure by Classification (%)",
            {"x": 3, "y": 4, "width": 3, "height": 5}, "Classification", "Avg Illiquid %", colors=[RED, AMBER, GREEN, TEAL, BLUE]),
        scatter("s_size", d_scatter, "total_investments_m", "illiquid_pct", "classification",
                "Illiquid Exposure vs Fund Size", {"x": 0, "y": 9, "width": 3, "height": 6},
                "Total Investments ($M)", "Illiquid %"),
        pie("pie_profit", d_profit, "total_inv_bn", "profit_status", "AUM by Profit Status ($B)",
            {"x": 3, "y": 9, "width": 3, "height": 6}),
        bar("b_fi", d_class, "classification", "avg_fi_pct", "Avg Fixed Income Allocation by Classification (%)",
            {"x": 0, "y": 15, "width": 3, "height": 5}, "Classification", "Avg Fixed Income %", colors=[TEAL, GREEN, BLUE, AMBER, SLATE]),
        bar("b_cash", d_class, "classification", "avg_cash_pct", "Avg Cash Allocation by Classification (%)",
            {"x": 3, "y": 15, "width": 3, "height": 5}, "Classification", "Avg Cash %", colors=[GREEN, TEAL, BLUE, AMBER, SLATE]),
        table("t_top", d_top, [
            ("fund_name", "string", "Fund"),
            ("classification", "string", "Classification"),
            ("illiquid_pct", "float", "Illiquid %", "0.0"),
            ("peer_avg_pct", "float", "Peer Avg %", "0.0"),
            ("vs_peer_pp", "float", "vs Peer (pp)", "+0.0;-0.0"),
            ("total_m", "float", "Total ($M)", "#,##0"),
            ("peer_comparison", "string", "Status"),
        ], "Funds with Highest Illiquid Asset Exposure vs Peers", {"x": 0, "y": 20, "width": 6, "height": 6}),
    ]
    layout2 += [
        text("p2head",
             "## Industry Asset Allocation Trends\n"
             "Total superannuation industry investments and the asset-class mix over time. "
             "Data: APRA Quarterly Superannuation Performance Statistics (Table 1d, industry asset allocation).",
             {"x": 0, "y": 0, "width": 6, "height": 2}),
        line("l_total", d_total, "quarter_date", "total_bn", "Total Industry Superannuation Investments Over Time ($B)",
             {"x": 0, "y": 2, "width": 6, "height": 6}, "Quarter", "Total Investments ($B)"),
        area("a_mix", d_mix, "quarter_date", "value_bn", "asset_class", "Asset Allocation Mix Over Time ($B)",
             {"x": 0, "y": 8, "width": 6, "height": 7}, "Quarter", "Allocation ($B)"),
    ]
    return {
        "datasets": datasets,
        "pages": [
            {"name": uid("page_super"), "displayName": "Fund Risk & Allocation",
             "pageType": "PAGE_TYPE_CANVAS", "layout": layout1},
            {"name": uid("page_trends"), "displayName": "Industry Trends",
             "pageType": "PAGE_TYPE_CANVAS", "layout": layout2},
        ],
        "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}, "applyModeEnabled": False},
    }


w = WorkspaceClient()

# Discover a serverless SQL warehouse (Free Edition ships one).
warehouses = list(w.warehouses.list())
serverless = [x for x in warehouses if getattr(x, "enable_serverless_compute", False)]
if not warehouses:
    raise RuntimeError("No SQL warehouse found. Open SQL → SQL Warehouses and start one, then re-run this cell.")
warehouse_id = (serverless or warehouses)[0].id
print(f"Using warehouse: {(serverless or warehouses)[0].name} ({warehouse_id})")

serialized = json.dumps(build_dashboard(FQ))

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
# MAGIC - 3 described tables · 1 metric view (`mv_super_fund_allocation`) · 1 analytical view (`v_super_illiquidity_peer`) · a published dashboard.
# MAGIC
# MAGIC **Your turn:** Go to **Genie → New**, scope it to the `workspace.data_day` schema, and try:
# MAGIC - *"Which funds have the highest illiquid asset exposure compared with their peers?"*
# MAGIC - *"What is the average equity allocation for public offer vs non-public offer funds?"*
# MAGIC - *"How has total industry superannuation AUM changed over time?"*
# MAGIC
# MAGIC Then improve it: add instructions, sample questions, and certified SQL. The table/column
# MAGIC comments and the metric view give Genie a strong head start.

# APRA Super Data Day

Hands-on lab assets for the APRA **Data Day**, built to run on **[Databricks Free Edition](https://www.databricks.com/learn/free-edition)**.

You'll stand up a small, governed superannuation dataset — three tables, a metric view, and an
AI/BI dashboard — and then use it to build and configure a **Genie space**.

All data is from **public** [APRA Quarterly Superannuation Statistics](https://www.apra.gov.au/quarterly-superannuation-statistics) and ships as Parquet (no Excel, no downloads).

---

## What you'll build

In the schema **`workspace.data_day`**:

| Object | Type | Description |
|--------|------|-------------|
| `super_fund_membership` | table | Fund-level membership profile (74 funds, Dec 2025) |
| `super_fund_asset_allocation` | table | Fund-level asset allocation in $M (74 funds, Dec 2025) |
| `super_performance` | table | Industry asset allocation over time (Dec 2004 – Dec 2025) |
| `mv_super_fund_allocation` | metric view | Governed measures: allocation %, illiquid %, AUM |
| **APRA Super Data Day** | dashboard | Fund Risk & Allocation + Industry Trends |

See [`data/SCHEMA.md`](data/SCHEMA.md) for the full data dictionary.

> **Optional extra:** [`sql/v_super_illiquidity_peer.sql`](sql/v_super_illiquidity_peer.sql) creates a
> per-fund *illiquid-vs-peers* view — a great asset for your Genie space. Run it after the notebook.
> (The dashboard doesn't need it — it inlines the same logic.)

---

## Run the lab

1. **Sign in** to Databricks Free Edition: <https://login.databricks.com/> (create a free account if needed).
2. **Add this repo as a Git folder:** in the left sidebar choose **Workspace → Create → Git folder**,
   paste this repository's URL, and clone it.
3. **Open** `notebooks/00_setup_data_day.py`.
4. At the top, set the compute to **Serverless**, then **Run all**. It takes ≈ 2–4 minutes
   (the first run also starts your serverless SQL warehouse).
5. When it finishes, the notebook prints a link to the **published dashboard**. Open it.

### Verify it worked
- `workspace.data_day` exists with **3 tables** (≈ 74 / 74 / 1,837 rows).
- Table and column descriptions are visible in **Catalog Explorer**.
- This returns rows (metric view):
  ```sql
  SELECT `Classification`, MEASURE(`Avg Illiquid Pct`)
  FROM workspace.data_day.mv_super_fund_allocation GROUP BY ALL;
  ```
- The **APRA Super Data Day** dashboard renders all tiles, charts and tables.

---

## Your exercise: build a Genie space

The point of the lab is to configure a great **Genie** experience on this data.

1. Go to **Genie → New** and scope it to the **`workspace.data_day`** schema
   (add the 3 tables and the `mv_super_fund_allocation` metric view — plus the
   `v_super_illiquidity_peer` view if you ran the optional SQL).
2. Ask a few questions:
   - *Which funds have the highest illiquid asset exposure compared with their peers?*
   - *What is the average equity allocation for public offer vs non-public offer funds?*
   - *How has total industry superannuation AUM changed over time?*
3. Improve it: add **general instructions**, **sample questions**, and **certified SQL**.
   The table/column comments and the `mv_super_fund_allocation` metric view already give Genie
   strong context — extend from there.

---

## Repo layout

```
.
├── notebooks/00_setup_data_day.py     # the one notebook you run
├── data/                              # APRA superannuation tables as Parquet + data dictionary
├── dashboard/super_data_day.lvdash.json  # the dashboard definition (the notebook publishes this file)
├── sql/v_super_illiquidity_peer.sql   # optional illiquid-vs-peers view for Genie
└── README.md
```

## Notes
- **Serverless only.** The notebook reads the bundled Parquet with pandas on the driver and writes
  managed Delta tables — Spark can't read Git-folder files directly on serverless.
- **Metric views** are GA on current Databricks serverless. (If you somehow land on an older runtime
  and cell 5 errors, just re-run on serverless.) The dashboard only needs the 3 tables, so it renders
  regardless.
- **Re-running is safe** — tables are overwritten and the dashboard is updated in place (no duplicates).
- The committed `dashboard/super_data_day.lvdash.json` opens as a draft straight from the Git folder
  (assign your warehouse when prompted); running the notebook is the recommended path as it publishes
  the dashboard for you.

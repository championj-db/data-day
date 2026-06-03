# APRA Super Data Day — Build the Demo Yourself

**Table of Contents**

- Before you start
- Step 1 — Sign up for Databricks Free Edition
- Step 2 — Add this repo as a Git folder
- Step 3 — Run the install notebook
- Step 4 — Start on the dashboard
- Step 5 — Create your Genie space
- Step 6 — Your first question (agent mode)
- Step 7 — The warm-up questions
- Step 8 — Make Genie smarter (curation)
  - 8a — Add a space description
  - 8b — Add general instructions
  - 8c — Add four example queries (the "trusted assets")
  - 8d — Add five sample questions (the welcome mat)
- Step 9 — The before-and-after moment
- Step 10 — The three "wow" questions (agent mode)
  - Aha #1 — find the unusual funds
  - Aha #2 — the "why" question
  - Aha #3 — the recommendation
- What you just built
- Where to take it next
- A note on what you did not build
- A final word

---

## Before you start

Everything you saw in the demo, you are about to build with your own hands. This tutorial walks you through it step by step. No SQL knowledge required. No prior Databricks experience needed.

**What you will build:** Your own Genie space that answers prudential questions about Australian superannuation data — **74 APRA-regulated funds** (their membership profiles and how they invest their members' money, as at December 2025) plus **20 years of industry-wide asset allocation** (December 2004 to December 2025). All of it sourced from the **public** [APRA Quarterly Superannuation Statistics](https://www.apra.gov.au/quarterly-superannuation-statistics).

**What you need:**

- A web browser
- An email address you can sign up with
- About 30–45 minutes

**What you will learn:**

- How to sign up for the free Databricks platform
- How to load a governed dataset by cloning a Git folder and running one notebook
- How to read an AI/BI dashboard built on that data
- How to create a Genie space and ask it questions in plain English
- How a few minutes of "curation" makes Genie noticeably smarter
- How to ask the kinds of questions a dashboard cannot answer — and have Genie draft a report

A note before you start: every screen and every button name in Databricks may shift slightly over time. If a label moves, look for the closest match. The flow does not change.

## Step 1 — Sign up for Databricks Free Edition

| **Step** | **What to do** |
| :-- | :-- |
| 1 | **Open your browser and go to [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition)** |
| 2 | **Click Get Started for Free (or Sign up)** |
| 3 | Enter your email address and a password |
| 4 | Confirm your email via the link Databricks sends you |
| 5 | **When asked, pick whichever cloud is offered first — it makes no practical difference for this tutorial** |
| 6 | Wait until your workspace finishes provisioning. You will land on the Databricks home page. |

**Tip:** Free Edition is exactly what its name says — free to use, no credit card. It is the same platform a Databricks customer uses, just with smaller compute and a different licence. You can sign back in any time at [login.databricks.com](https://login.databricks.com/).

## Step 2 — Add this repo as a Git folder

Instead of downloading and uploading a file, you'll clone a small Git repository straight into your workspace. It contains the data, the setup notebook, and a ready-made dashboard.

| **Step** | **What to do** |
| :-- | :-- |
| 1 | **In the left sidebar, click Workspace** |
| 2 | **Click Create (top right), then choose Git folder** |
| 3 | **In the Git repository URL field, paste:** `https://github.com/championj-db/data-day.git` |
| 4 | Leave the Git provider as GitHub and the branch as `main` |
| 5 | **Click Create Git folder** |

After a few seconds you'll see a `data-day` folder in your workspace with `notebooks/`, `data/`, `dashboard/` and `sql/` inside it. That `data/` folder holds the APRA superannuation tables (as Parquet files) — no spreadsheets to wrangle.

## Step 3 — Run the install notebook

This one notebook builds everything the lab needs. It replaces the old "upload a CSV" step entirely.

| **Step** | **What to do** |
| :-- | :-- |
| 1 | **Open** `data-day/notebooks/00_setup_data_day.py` |
| 2 | **At the top of the notebook, set the compute to Serverless** (Free Edition gives you this by default) |
| 3 | **Click Run all** |
| 4 | Wait ≈ 2–4 minutes. The first run also starts your serverless SQL warehouse. |
| 5 | **When it finishes, the last cell prints a link to the published dashboard. Keep that link handy.** |

**What the notebook just did.** In a schema called **`workspace.data_day`** it created:

| Object | Type | What it is |
| :-- | :-- | :-- |
| `super_fund_membership` | table | Fund-level membership profile — 74 funds, Dec 2025 |
| `super_fund_asset_allocation` | table | Fund-level asset allocation in $M AUD — 74 funds, Dec 2025 |
| `super_performance` | table | Industry-wide asset allocation over time — Dec 2004 to Dec 2025 |
| `mv_super_fund_allocation` | metric view | Governed measures: allocation %, illiquid %, total AUM |
| **APRA Super Data Day** | dashboard | Fund Risk & Allocation + Industry Trends |

It also wrote rich **descriptions** onto every table and key column. That matters — Genie reads those descriptions as context later.

**Verify it worked (optional, 30 seconds).** Open a SQL editor or a new notebook cell and check:

- `workspace.data_day` exists with **3 tables** (≈ 74 / 74 / 1,837 rows).
- This returns rows (it proves the metric view is live):

  ```sql
  SELECT `Classification`, MEASURE(`Avg Illiquid Pct`)
  FROM workspace.data_day.mv_super_fund_allocation
  GROUP BY ALL;
  ```

## Step 4 — Start on the dashboard

Before we talk to the data in plain English, let's see what it looks like on a fixed canvas. Open the **APRA Super Data Day** dashboard using the link the notebook printed (or find it under **Dashboards** in the left sidebar).

**Take a few minutes to look around the two pages:**

**Page 1 — Fund Risk & Allocation**

- Three KPI tiles at the top: **number of super funds** (74), **total investments ($B)**, and **average illiquid exposure (%)**.
- Bar charts comparing **asset allocation by regulatory classification** (public offer vs non-public offer) — average equity, illiquid, fixed income and cash exposure.
- A **scatter plot** of illiquid exposure vs fund size — does being bigger mean holding more illiquid assets?
- A **pie chart** of AUM split by profit status (for-profit vs not-for-profit licensees).
- A **table** of the funds with the highest illiquid exposure compared with their peers.

**Page 2 — Industry Trends**

- A **line chart** of total industry superannuation investments from Dec 2004 to Dec 2025 — the long-run growth story.
- An **area chart** of the asset-allocation mix over time — how the industry's split across equity, fixed income, cash, property, infrastructure and other has shifted.

A dashboard is excellent at answering the questions someone already thought to put on it. But the moment you have a *new* question — "which funds are the outliers, and why?" — you'd normally file a ticket and wait. That's the gap Genie closes. Let's build it.

## Step 5 — Create your Genie space

A Genie space is the "room" where you have conversations with your data.

| **Step** | **What to do** |
| :-- | :-- |
| 1 | **On the left sidebar, click Genie** (it may sit under AI/BI) |
| 2 | **Click + New (or Create Genie space)** |
| 3 | **Give your space a name. Suggested:** `YOUR FIRSTNAME - APRA Super` |
| 4 | **When asked for data, scope it to the `workspace.data_day` schema** |
| 5 | **Add all four datasets:** `super_fund_membership`, `super_fund_asset_allocation`, `super_performance`, and the `mv_super_fund_allocation` metric view |
| 6 | **For the SQL warehouse, pick whichever one is offered.** On Free Edition there is usually a default. |
| 7 | **Click Create** |

Your space is live. You should land in a chat interface with a blank chat box.

## Step 6 — Your first question (agent mode)

Before we warm up, let's go straight for the big one — the kind of task you'd normally hand to an analyst and wait a week for.

| **Step** | **What to do** |
| :-- | :-- |
| 1 | **Switch the space into agent mode** (look for the mode toggle near the chat box — sometimes labelled "agent" or "deep research") |
| 2 | **Type this question exactly, and press Enter:** |

> **Identify the trends in the data and prepare a senate estimate report**

Then sit back and watch.

**What you will see:** Genie treats this as a multi-step assignment, not a single query. It will explore the tables, run several queries, and assemble a structured, narrative report — typically covering:

- How total industry superannuation investment has grown over the last two decades (`super_performance`)
- How the asset-allocation mix has shifted across equity, fixed income, property, infrastructure and alternatives
- The spread of illiquid-asset exposure across today's 74 funds, and which segments carry the most
- A plain-English summary written the way you'd brief an executive — or answer a question on notice at **Senate Estimates**

This is the moment to pause and appreciate it: a prudential analyst would spend the better part of a day pulling this together. You just got a first draft in under a minute, with the SQL behind every figure available to inspect.

Now let's understand *how* it got there — and make every future answer sharper. **Continue.**

## Step 7 — The warm-up questions

Try these one at a time. Just type each question into the chat box and press Enter (default mode is fine here). Wait for Genie to finish — it usually takes 5–15 seconds.

| **#** | **Type this question** | **What you should see** |
| :-- | :-- | :-- |
| 1 | What is in this dataset? Give me a quick summary. | A short description of the tables — fund membership, fund asset allocation, and industry allocation over time |
| 2 | How many funds are there, and what is the total amount invested? | About 74 funds, with total investments in the trillions of dollars (reported in $M / $B AUD) |
| 3 | What is the total amount invested, split by fund type? | A small table across Retail, Industry, Public Sector and Corporate funds |
| 4 | Show me how total industry superannuation investment has changed over time. | A line or bar chart rising steadily from 2004 to 2025 |
| 5 | What is the average illiquid asset exposure across funds, and which classification is highest? | An average illiquid %, broken down by public offer vs non-public offer |

**Stop and look at the "Show generated query" toggle.** Click it on any answer. You will see the actual SQL Genie wrote. That is the transparency that makes Genie auditable — every figure traces back to a query you and your data team can read.

## Step 8 — Make Genie smarter (curation)

This is where the demo becomes interesting. We are going to teach Genie a few things, in four short steps. Each step takes a minute or two.

### 8a — Add a space description

| **Step** | **What to do** |
| :-- | :-- |
| 1 | **In the top right of your Genie space, click the gear or Settings icon** |
| 2 | **Find the Description field** |
| 3 | Paste the following text into it, then click Save: |

> APRA-regulated superannuation fund data for prudential analysis. 74 funds (December 2025) with membership profiles and fund-level asset allocation in $M AUD, plus industry-wide quarterly asset allocation from December 2004 to December 2025. Use this space to ask plain-English questions about asset allocation, illiquid-asset exposure and peer comparisons, fund risk profiles, and long-run industry trends. Source: public APRA Quarterly Superannuation Statistics.

### 8b — Add general instructions

This is where you teach Genie the domain language and the quirks of the data — the things that would otherwise trip it up.

| **Step** | **What to do** |
| :-- | :-- |
| 1 | **Still in space settings, find the section called Instructions (sometimes General instructions)** |
| 2 | **Click Add instruction** |
| 3 | Paste the text below |
| 4 | Save |

```
You are an APRA prudential analyst helping supervisors and risk specialists explore
Australian superannuation data. Be precise, cite the figures, and keep a regulatory tone.

KEY DEFINITIONS
- Illiquid assets = unlisted property + unlisted infrastructure + alternatives + private debt.
  In super_fund_asset_allocation this is:
  au_unlisted_property_m + intl_unlisted_property_m + au_unlisted_infrastructure_m
  + intl_unlisted_infrastructure_m + alternatives_m + private_debt_m.
- AUM ("assets under management", "total investments", "fund size") = total_fund_investments_m.
- RSE = Registrable Superannuation Entity — the legal entity that governs a super fund.
- ABN = Australian Business Number (11-digit fund identifier).
- Regulatory classification: "public offer" (open to anyone) vs "non-public offer"
  (restricted membership, e.g. an employer group).
- Fund type: Retail, Industry, Public Sector, or Corporate.

CALCULATION RULES
- Illiquid % = illiquid_assets_m / total_fund_investments_m * 100. Only count funds where
  total_fund_investments_m > 0.
- For aggregate allocation questions ("average illiquid % by classification", "total AUM"),
  PREFER the metric view mv_super_fund_allocation and query it with MEASURE(...).

TABLE GUIDE
- super_fund_membership: one row per fund (Dec 2025) — membership accounts, balances, demographics.
- super_fund_asset_allocation: one row per fund (Dec 2025) — asset allocation in $M AUD.
- super_performance: industry-wide allocation over time, LONG format (quarter, asset_class, value_m).
- mv_super_fund_allocation: governed measures (allocation %, illiquid %, AUM) over the fund-level data.

DATA CAVEATS
- super_performance mixes dollar-value rows with proportion rows (value_m < 1) and subtotal rows.
  Filter value_m > 10 to keep dollar amounts. The seven top-level classes (Equity, Fixed income,
  Cash, Property, Infrastructure, Other, Commodities) sum to 'Total investments'. Parse the period
  with try_to_date(quarter, 'MMM yyyy') for any time series.
- In super_fund_membership several numeric-looking columns are stored as text because the source
  publication uses markers (ranges, suppressed values). Treat them with care.

MONETARY VALUES & FORMATTING
- Columns ending _m are in $M AUD; metric-view totals (…Bn) are in $B AUD.
- Prefix dollar amounts with AUD. Show percentages to 1 decimal place.
- When ranking outlier funds, sort by total assets (largest first).
```

### 8c — Add four example queries (the "trusted assets")

Genie learns by example. We are going to give it four canonical query patterns. Once these are saved, whenever someone asks one of these kinds of questions, Genie reuses the trusted pattern — and the answer carries the "Trusted" badge.

For each of the four below, do this:

| **Step** | **What to do** |
| :-- | :-- |
| 1 | **In settings, find Example SQL queries (sometimes Sample SQL or Example questions)** |
| 2 | **Click Add example** |
| 3 | **Paste the question into the question field** |
| 4 | **Paste the SQL into the SQL field** |
| 5 | Save |

**Example 1 — allocation & illiquid exposure by classification**

Question:

```
What is the asset allocation and illiquid exposure for public offer vs non-public offer funds?
```

SQL:

```sql
SELECT `Classification`,
       MEASURE(`Fund Count`)           AS fund_count,
       MEASURE(`Total Investments Bn`) AS total_investments_bn,
       MEASURE(`Avg Illiquid Pct`)     AS avg_illiquid_pct,
       MEASURE(`Avg Equity Pct`)       AS avg_equity_pct,
       MEASURE(`Avg Fixed Income Pct`) AS avg_fixed_income_pct,
       MEASURE(`Avg Cash Pct`)         AS avg_cash_pct
FROM workspace.data_day.mv_super_fund_allocation
GROUP BY ALL
ORDER BY total_investments_bn DESC
```

**Example 2 — industry AUM trend over time (clean)**

Question:

```
How has total industry superannuation investment changed over time?
```

SQL:

```sql
SELECT try_to_date(quarter, 'MMM yyyy') AS quarter_date,
       value_m AS total_investments_m
FROM workspace.data_day.super_performance
WHERE asset_class = 'Total investments'
  AND value_m > 10
ORDER BY quarter_date
```

**Example 3 — funds with the highest illiquid exposure vs their peers**

Question:

```
Which funds have the highest illiquid asset exposure compared with their peers?
```

SQL:

```sql
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
    FROM workspace.data_day.super_fund_asset_allocation
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
ORDER BY f.illiquid_pct DESC
```

**Example 4 — average allocation by fund type**

Question:

```
What is the average asset allocation by fund type?
```

SQL:

```sql
SELECT `Fund Type`,
       MEASURE(`Fund Count`)            AS fund_count,
       MEASURE(`Avg Equity Pct`)        AS avg_equity_pct,
       MEASURE(`Avg Fixed Income Pct`)  AS avg_fixed_income_pct,
       MEASURE(`Avg Property Pct`)      AS avg_property_pct,
       MEASURE(`Avg Infrastructure Pct`) AS avg_infrastructure_pct,
       MEASURE(`Avg Alternatives Pct`)  AS avg_alternatives_pct,
       MEASURE(`Avg Illiquid Pct`)      AS avg_illiquid_pct
FROM workspace.data_day.mv_super_fund_allocation
GROUP BY ALL
ORDER BY avg_illiquid_pct DESC
```

### 8d — Add five sample questions (the welcome mat)

These appear when someone opens your space for the first time. They tell new visitors "here is what this space is good at."

| **Step** | **What to do** |
| :-- | :-- |
| 1 | **In settings, find Sample questions** |
| 2 | Add each of these as a separate sample question |

1. What is in this dataset? Give me a quick summary.
2. What is the asset allocation for public offer vs non-public offer funds?
3. How has total industry superannuation investment changed over time?
4. Which funds have the highest illiquid asset exposure compared with their peers?
5. How has the industry's asset-allocation mix shifted since 2004?

Click **Save** and return to the chat.

## Step 9 — The before-and-after moment

Now we will see the curation pay off. Ask Genie this question again — the same wording you may have tried during warm-up:

> **How has total industry superannuation investment changed over time?**

Compare the SQL it writes now to the SQL it would have written before curation.

| **Before curation** | **After curation** |
| :-- | :-- |
| Genie sees `super_performance` is in long format and may pick up the proportion rows (`value_m < 1`) and subtotal rows, or mis-read the text `quarter` — the trend comes out noisy or wrong. | Genie filters `value_m > 10`, restricts to `asset_class = 'Total investments'`, and parses the quarter with `try_to_date(...)` — a clean, monotonic growth line. |

That is what a single instruction and a single example query did. A few minutes of curation, and every future answer is better.

## Step 10 — The three "wow" questions (agent mode)

Now you ask the kinds of questions a dashboard literally cannot answer. Switch the space into **agent mode** and type each of these. Watch Genie reason through them.

### Aha #1 — find the unusual funds

Question to type:

> **Which funds stand out as unusual on their risk profile — either unusually high illiquid exposure I should scrutinise, or unusually conservative outliers? Tell me what makes each one unusual: regulatory classification, fund type, profit status, fund size, and its illiquid % versus its peer segment.**

What you will see:

- Genie computes each fund's illiquid %, the peer-segment average and spread, then flags the funds sitting well above or below their peers
- It returns the outlier funds with the **reason** (high illiquid exposure, very conservative, large vs small, etc.)
- It explains the unifying pattern across the outliers

This is a multi-step reasoning task. A prudential analyst would take half a day. You just got it in seconds — and it's the heart of a supervision watchlist.

### Aha #2 — the "why" question

Question to type:

> **The industry has shifted its allocation across asset classes over the last two decades. What's driving that? Break the change down by asset class — equity, fixed income, property, infrastructure, alternatives, cash — and tell me which classes grew the most and which shrank.**

What you will see:

- Genie decomposes the change in the industry's allocation mix over time (`super_performance`)
- It returns the asset classes that gained and lost the most share
- It writes a one-paragraph summary in plain English

Most dashboards tell you *what* the mix is today. This tells you *how it got here* — and *why* it matters for liquidity risk.

### Aha #3 — the recommendation

Question to type:

> **From a prudential supervision standpoint, which funds and which segments warrant closer attention on liquidity risk, and why? Compare each fund's illiquid exposure to its peer segment, then give me a ranked watchlist with the reasoning written out.**

What you will see:

- Genie compares every fund's illiquid exposure to its peer segment
- It returns a **ranked watchlist** with the reasoning spelled out
- It may even ask a follow-up: *"Would you like me to weight this by fund size, or by how far each fund sits above its peers?"*

This is the moment Genie stops feeling like a tool and starts feeling like a colleague — exactly the colleague you'd want beside you the night before a Senate Estimates hearing.

## What you just built

In about thirty minutes you have:

- A real Databricks workspace
- A real, governed dataset — three superannuation tables and a metric view — loaded by cloning a Git folder and running one notebook
- A published AI/BI dashboard on that data
- A real Genie space, governed by the same access controls Databricks uses for every other piece of data
- A space that has been *curated* — a description, rich domain instructions, four trusted example queries, and five sample questions

Every question you ask now produces an answer with the SQL shown, the data attached, and the full audit trail captured. You can put it in front of an executive — or a Senate committee — tomorrow.

## Where to take it next

Pick **one** of these:

1. **Replace the data.** Export one of your team's own reports. Load it into a schema, point a Genie space at it, and ask the questions your stakeholders ask you every week. Show them the answers in your next meeting.
2. **Add more tables.** Genie can answer across joined tables. Try adding the optional `sql/super_illiquidity_peer.sql` view from the repo, or bring in another APRA publication, and ask questions that span both.
3. **Practice curation.** Pick a definition your team debates (e.g. "what exactly counts as an illiquid asset?"). Write it as an instruction in your space. Watch every future answer respect it.

## A note on what you did not build

You did not train an AI. You did not give a model your data to keep. You did not bypass anyone's permissions. You did not create a "shadow" copy of your data.

You created a space that:

- Reads from governed tables inside your workspace
- Inherits the access rules that already protect those tables
- Generates real SQL that you and your data team can read and audit
- Logs every question, answer, and result for compliance

That is the reliability story. That is why the same approach works for prudential supervision, healthcare, finance and the wider public service — the technology never bypasses governance, it inherits it.

## A final word

Take the space you just built back to your team. Curate it for one of your real datasets. Show three people. The conversation about "what AI can actually do for us" gets a lot more concrete when you have a working example to point at — built on data your colleagues already trust.

Good luck.

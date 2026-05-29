# Data dictionary

All data is sourced from **public** APRA statistical publications:
<https://www.apra.gov.au/quarterly-superannuation-statistics>

The tables are shipped as Parquet (a dump of the curated demo tables) so the lab notebook
loads them directly — **no Excel parsing required**. Values in columns ending `_m` are in
**$ millions AUD**.

| File | Table | Rows | Grain |
|------|-------|------|-------|
| `super_fund_membership.parquet` | `super_fund_membership` | 74 | one row per fund (Dec 2025) |
| `super_fund_asset_allocation.parquet` | `super_fund_asset_allocation` | 74 | one row per fund (Dec 2025) |
| `super_performance.parquet` | `super_performance` | 1,837 | one row per quarter × asset class (Dec 2004 – Dec 2025) |

---

## `super_fund_membership`
Fund-level membership profile. Source: APRA Quarterly Superannuation Fund Statistics, **Table 1**.

| Column | Type | Notes |
|--------|------|-------|
| `period` | date | Reporting quarter end |
| `fund_name` | string | Fund (RSE) name |
| `abn` | bigint | Australian Business Number |
| `rse_regulatory_classification` | string | `public offer` / `non-public offer` |
| `fund_type` | string | Retail / Industry / Public Sector / Corporate |
| `rse_membership_base` | string | Membership base category |
| `rse_licensee` | string | RSE licensee name |
| `rse_licensee_ownership_type` | string | Licensee ownership type |
| `rse_licensee_profit_status` | string | `For profit status` / `Not for profit status` |
| `rse_licensee_board_structure` | string | Board structure |
| `total_member_accounts` | string | Total member accounts |
| `total_members_benefits_000` | string | Total member benefits ($'000) |
| `median_benefit_bracket` | string | Median benefit bracket |
| `estimated_median_account_balance` | string | Estimated median account balance |
| `median_member_age` | string | Median member age |
| `active_member_accounts` | string | Active member accounts |
| `inactive_member_accounts` | string | Inactive member accounts |
| `active_members_benefits_000` | string | Active member benefits ($'000) |
| `inactive_members_benefits_000` | string | Inactive member benefits ($'000) |
| `avg_active_account_balance_000` | string | Avg active account balance ($'000) |
| `avg_inactive_account_balance_000` | string | Avg inactive account balance ($'000) |

> Several numeric-looking columns are typed `string` because the source publication uses text
> markers (e.g. ranges, suppressed values). They are preserved as published.

## `super_fund_asset_allocation`
Fund-level asset allocation across asset classes. Source: APRA Quarterly Superannuation Fund
Statistics, **Table 4**. The first 10 columns match `super_fund_membership`; the remainder are
allocation amounts in **$M AUD** (`double`):

`cash_m`, `cash_derivative_offset_m`, `cash_fx_m`, `fixed_income_m`, `au_fixed_income_m`,
`intl_fixed_income_m`, `fixed_income_other_m`, `private_debt_m`, `equity_m`, `au_listed_equity_m`,
`intl_listed_equity_hedged_m`, `intl_listed_equity_unhedged_m`, `au_unlisted_equity_m`,
`intl_unlisted_equity_hedged_m`, `intl_unlisted_equity_unhedged_m`, `equity_other_m`, `property_m`,
`au_listed_property_m`, `intl_listed_property_m`, `au_unlisted_property_m`, `intl_unlisted_property_m`,
`property_other_m`, `infrastructure_m`, `au_listed_infrastructure_m`, `intl_listed_infrastructure_m`,
`au_unlisted_infrastructure_m`, `intl_unlisted_infrastructure_m`, `infrastructure_other_m`,
`alternatives_m`, `au_alternatives_m`, `intl_alternatives_m`, `alternatives_other_m`,
`of_which_commodities_m`, `total_fund_investments_m`.

**Illiquid assets** (used throughout the lab) = `au_unlisted_property_m` + `intl_unlisted_property_m`
+ `au_unlisted_infrastructure_m` + `intl_unlisted_infrastructure_m` + `alternatives_m` + `private_debt_m`.
`total_fund_investments_m` is the portfolio denominator.

## `super_performance`
Quarterly **industry-wide** asset allocation, long format. Source: APRA Quarterly Superannuation
Performance Statistics, **Table 1d**.

| Column | Type | Notes |
|--------|------|-------|
| `quarter` | string | e.g. `Dec 2025`; parse with `try_to_date(quarter,'MMM yyyy')` |
| `asset_class` | string | See note below |
| `value_m` | double | $M AUD for dollar rows; proportion rows are `< 1` |

> This table mixes **dollar-value rows** and **proportion rows** (the same class as a fraction),
> plus subtotal rows (`Total investments`, `Equity`, `Fixed income`, …) and a `Number of entities`
> row. The seven top-level classes — `Equity`, `Fixed income`, `Cash`, `Property`, `Infrastructure`,
> `Other`, `Commodities` — sum to `Total investments`. Filter `value_m > 10` to exclude proportions
> when charting dollar amounts.

---

*Refreshing the data:* re-run the maintainer dump that produced these files from the source
`au_pubsec_catalog.apra.super_*` tables (see the engagement notes). Participants never need to do this.

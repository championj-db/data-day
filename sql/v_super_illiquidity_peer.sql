-- v_super_illiquidity_peer  ·  optional Genie / analysis asset for APRA Super Data Day
--
-- Per-fund illiquid-asset exposure (% of portfolio) with peer-segment averages.
-- Illiquid = unlisted property + unlisted infrastructure + alternatives + private debt.
-- Great for the flagship Genie question: "Which funds have the highest illiquid
-- exposure compared with their peers?"
--
-- Run this in a SQL editor (or a notebook cell) AFTER running
-- notebooks/00_setup_data_day.py, which creates the workspace.data_day tables.
-- The AI/BI dashboard does NOT need this view — it inlines the same logic.

CREATE OR REPLACE VIEW workspace.data_day.v_super_illiquidity_peer AS
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
JOIN segment_avg s ON f.rse_regulatory_classification = s.rse_regulatory_classification;

COMMENT ON TABLE workspace.data_day.v_super_illiquidity_peer IS
'Fund-level illiquid asset exposure (% of portfolio) with peer-segment averages. Illiquid = unlisted property + unlisted infrastructure + alternatives + private debt. Source: APRA Quarterly Superannuation Fund Statistics Table 4.';

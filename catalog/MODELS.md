# Warehouse semantic catalog and model reference

Auto-generated from `dbt/target/manifest.json` and live warehouse metadata by `src/catalog_sync.py`.

## Canonical metric definitions

| Metric | Source table | Formula | Rule |
|---|---|---|---|
| `active_loan_commitment_cad` | `marts.fct_applicant_milestones` | `SUM(approved_principal_cad)` | Sums approved principal across active non-superseded applications only; excludes superseded intake deferrals and declined underwriting. |
| `net_settled_fee_revenue_cad` | `marts.fct_payment_fee_ledger` | `SUM(signed_fee_revenue_cad)` | Sums settled application processing and origination fees minus reversed/refunded fee entries. |
| `position_based_cac_cad` | `marts.fct_marketing_attribution` | `SUM(ad_spend_cad) / SUM(CASE WHEN is_funded_loan THEN position_based_weight ELSE 0 END)` | Allocates 40% credit to first touch, 40% to last pre-signup touch, and 20% to middle touches across deduplicated click IDs. |
| `cohort_funded_conversion_rate_pct` | `marts.agg_cohort_funnel_kpis` | `100.0 * SUM(CASE WHEN is_funded THEN 1 ELSE 0 END) / COUNT(DISTINCT applicant_id)` | Measured on canonical root applicants assigned to their active post-deferral intake cohort. |

## Models and explicit grain

| Model | Schema | Materialized | Rows | Explicit grain | Description |
|---|---|---|---:|---|---|
| `agg_cohort_funnel_kpis` | `marts` | `table` | 55 | 1 row per (active_intake_cohort, province_code, program_category) | Executive cohort conversion funnel and financial KPI aggregate table by intake cohort, province, and program category. |
| `dim_applicant_scd2` | `dimensions` | `table` | 4,500 | 1 row per (applicant_id, version_num) SCD Type 2 state interval | Kimball SCD Type 2 applicant dimension tracking point-in-time KYC, risk tier, and study permit changes. |
| `dim_loan_status_scd2` | `dimensions` | `table` | 5,400 | 1 row per (loan_id, version_num) SCD Type 2 underwriting stage interval | Kimball SCD Type 2 loan contract dimension tracking underwriting and disbursement state transitions. |
| `dim_program_institution` | `dimensions` | `table` | 30 | 1 row per partner Designated Learning Institution (institution_id) | Conformed Kimball dimension for partner post-secondary Designated Learning Institutions (DLIs). |
| `fct_applicant_milestones` | `marts` | `table` | 1,500 | 1 row per canonical applicant_id (active intake cohort, active guarantor, funded loan) | Kimball fact table at explicit 1-row-per-applicant grain combining SCD2 current state, resolved intake deferral lineage, active guarantor, screening outcomes, and reconciled loan/fee ledger totals. |
| `fct_loan_stage_transitions` | `marts` | `table` | 5,400 | 1 row per loan_version_sk stage interval with hours_in_stage | Kimball accumulating/transactional stage fact table over SCD Type 2 loan contract status versions. |
| `fct_marketing_attribution` | `marts` | `table` | 6 | 1 row per acquisition channel_name with CAC and ROAS across 3 attribution models | Channel-level marketing attribution mart comparing First-Touch, Last-Touch, and Position-Based (40/20/40) funded loans, CAC, and ROAS. |
| `fct_payment_fee_ledger` | `marts` | `table` | 5,195 | 1 row per payment_id ledger entry | Kimball transaction-grain ledger fact table for application fees, reversals, and tuition/stipend disbursements. |
| `int_ad_click_attribution` | `core` | `table` | 3,500 | 1 row per deduplicated click_id with First-Touch, Last-Touch, and Position-Based weights | Assigns First-Touch, Last-Touch, and Position-Based (40/20/40) attribution weights across stitched ad clicks. |
| `int_deferral_lineage_chains` | `core` | `table` | 1,920 | 1 row per application_id with root_application_id and is_superseded_by_deferral | Links multi-term intake deferrals (Fall -> Winter -> Fall) to root_application_id and flags superseded applications. |
| `int_guarantor_replacement_chains` | `core` | `table` | 1,810 | 1 row per guarantor_id with active_guarantor_id and is_superseded_guarantor | Resolves co-signer/guarantor replacement chains and flags rejected or tombstoned guarantors. |
| `int_payment_fee_reconciliation` | `core` | `table` | 5,195 | 1 row per payment_id with reversal links and signed_fee_revenue_cad | Enriches fee and disbursement ledger rows with reversal links and signed net fee revenue. |
| `int_screening_outcomes` | `core` | `table` | 1,500 | 1 row per (applicant_id, application_id) aggregated across 3 check types | Aggregates academic, document authenticity, and financial screening checks to one row per applicant. |
| `srv_applicant_360` | `serving` | `table` | 1,500 | 1 row per applicant_id (Hasura GraphQL serving table with lineage breadcrumbs) | Dedicated serving layer table exposed via Hasura GraphQL for internal product ops tools, providing 1-row-per-applicant 360 state and lineage breadcrumbs. |
| `srv_ops_workqueue` | `serving` | `table` | 358 | 1 row per work_item_id (Hasura GraphQL operational review queue) | Prioritized operational workqueue exposed via Hasura GraphQL for manual deferral lineage review, guarantor replacement audits, and fee reversal checks. |
| `stg_ad_clicks_and_utms` | `staging` | `view` | 3,500 | 1 row per deduplicated click_id stitched to applicant_id | Deduplicated ad click telemetry stitched to applicant_id across anonymous browser sessions. |
| `stg_cdc_applicants` | `staging` | `view` | 1,500 | 1 row per canonical applicant_id (latest compacted CDC LSN) | Latest compacted applicant state per applicant_id from Debezium Postgres CDC. |
| `stg_cdc_applications` | `staging` | `view` | 1,920 | 1 row per live application_id (tombstone deletes excluded) | Compacted live academic and loan applications with tombstone deletes removed and parent deferral links resolved. |
| `stg_cdc_guarantors` | `staging` | `view` | 1,810 | 1 row per guarantor_id (all co-signer submissions and replacement pointers) | Compacted guarantor and co-signer records including replacement pointers and tombstone flags. |
| `stg_cdc_loan_contracts` | `staging` | `view` | 1,500 | 1 row per loan_id (latest compacted CDC LSN) | Latest compacted loan contract state per loan_id. |
| `stg_cdc_payments_and_fees` | `staging` | `view` | 5,195 | 1 row per payment_id (fee collections, reversals, and disbursements) | Compacted fee payments, reversals, and loan disbursements. |
| `stg_cdc_screening_checks` | `staging` | `view` | 4,500 | 1 row per screening_id (academic, document, financial check) | Compacted screening checks (academic, document authenticity, financial capacity). |

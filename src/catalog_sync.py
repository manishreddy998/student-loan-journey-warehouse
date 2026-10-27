"""Auto-sync dbt manifest, model documentation, grain specs, and Hasura GraphQL metadata."""
from __future__ import annotations

import json
import time
import yaml

from src.config import CATALOG_DIR, HASURA_DIR, ROOT
from src.warehouse import estimate_bigquery_scan_discipline, read_sql

EXPLICIT_GRAINS = {
    "stg_cdc_applicants": "1 row per canonical applicant_id (latest compacted CDC LSN)",
    "stg_cdc_applications": "1 row per live application_id (tombstone deletes excluded)",
    "stg_cdc_screening_checks": "1 row per screening_id (academic, document, financial check)",
    "stg_cdc_loan_contracts": "1 row per loan_id (latest compacted CDC LSN)",
    "stg_cdc_guarantors": "1 row per guarantor_id (all co-signer submissions and replacement pointers)",
    "stg_cdc_payments_and_fees": "1 row per payment_id (fee collections, reversals, and disbursements)",
    "stg_ad_clicks_and_utms": "1 row per deduplicated click_id stitched to applicant_id",
    "dim_applicant_scd2": "1 row per (applicant_id, version_num) SCD Type 2 state interval",
    "dim_loan_status_scd2": "1 row per (loan_id, version_num) SCD Type 2 underwriting stage interval",
    "dim_program_institution": "1 row per partner Designated Learning Institution (institution_id)",
    "int_deferral_lineage_chains": "1 row per application_id with root_application_id and is_superseded_by_deferral",
    "int_guarantor_replacement_chains": "1 row per guarantor_id with active_guarantor_id and is_superseded_guarantor",
    "int_screening_outcomes": "1 row per (applicant_id, application_id) aggregated across 3 check types",
    "int_payment_fee_reconciliation": "1 row per payment_id with reversal links and signed_fee_revenue_cad",
    "int_ad_click_attribution": "1 row per deduplicated click_id with First-Touch, Last-Touch, and Position-Based weights",
    "fct_applicant_milestones": "1 row per canonical applicant_id (active intake cohort, active guarantor, funded loan)",
    "fct_loan_stage_transitions": "1 row per loan_version_sk stage interval with hours_in_stage",
    "fct_payment_fee_ledger": "1 row per payment_id ledger entry",
    "fct_marketing_attribution": "1 row per acquisition channel_name with CAC and ROAS across 3 attribution models",
    "agg_cohort_funnel_kpis": "1 row per (active_intake_cohort, province_code, program_category)",
    "srv_applicant_360": "1 row per applicant_id (Hasura GraphQL serving table with lineage breadcrumbs)",
    "srv_ops_workqueue": "1 row per work_item_id (Hasura GraphQL operational review queue)",
}

CANONICAL_METRICS = [
    {
        "metric_name": "active_loan_commitment_cad",
        "model": "marts.fct_applicant_milestones",
        "sql_expression": "SUM(approved_principal_cad)",
        "business_rule": "Sums approved principal across active non-superseded applications only; excludes superseded intake deferrals and declined underwriting.",
    },
    {
        "metric_name": "net_settled_fee_revenue_cad",
        "model": "marts.fct_payment_fee_ledger",
        "sql_expression": "SUM(signed_fee_revenue_cad)",
        "business_rule": "Sums settled application processing and origination fees minus reversed/refunded fee entries.",
    },
    {
        "metric_name": "position_based_cac_cad",
        "model": "marts.fct_marketing_attribution",
        "sql_expression": "SUM(ad_spend_cad) / SUM(CASE WHEN is_funded_loan THEN position_based_weight ELSE 0 END)",
        "business_rule": "Allocates 40% credit to first touch, 40% to last pre-signup touch, and 20% to middle touches across deduplicated click IDs.",
    },
    {
        "metric_name": "cohort_funded_conversion_rate_pct",
        "model": "marts.agg_cohort_funnel_kpis",
        "sql_expression": "100.0 * SUM(CASE WHEN is_funded THEN 1 ELSE 0 END) / COUNT(DISTINCT applicant_id)",
        "business_rule": "Measured on canonical root applicants assigned to their active post-deferral intake cohort.",
    },
]


def sync_catalog_and_hasura() -> dict:
    t0 = time.time()
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)
    (HASURA_DIR / "metadata").mkdir(parents=True, exist_ok=True)

    manifest_path = ROOT / "dbt" / "target" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {"nodes": {}}

    models_catalog = []
    for node_id, node in sorted(manifest.get("nodes", {}).items()):
        if node.get("resource_type") != "model":
            continue
        name = node["name"]
        schema = node["schema"]
        full_table = f"{schema}.{name}"
        try:
            row_cnt = int(read_sql(f"SELECT COUNT(*) AS c FROM {full_table}").iloc[0]["c"])
        except Exception:
            row_cnt = 0
        cols = [
            {
                "name": col_name,
                "description": col_meta.get("description", ""),
            }
            for col_name, col_meta in node.get("columns", {}).items()
        ]
        models_catalog.append(
            {
                "model_name": name,
                "schema": schema,
                "relation": full_table,
                "materialized": node.get("config", {}).get("materialized", "table"),
                "explicit_grain": EXPLICIT_GRAINS.get(name, "1 row per primary key"),
                "description": node.get("description", ""),
                "row_count": row_cnt,
                "columns": cols,
            }
        )

    scan_discipline = estimate_bigquery_scan_discipline()
    context_payload = {
        "warehouse": "student_loan_journey",
        "model_count": len(models_catalog),
        "canonical_metrics": CANONICAL_METRICS,
        "bigquery_partition_cluster_discipline": scan_discipline,
        "models": models_catalog,
    }
    (CATALOG_DIR / "warehouse_context.json").write_text(json.dumps(context_payload, indent=2), encoding="utf-8")

    # Write human-readable catalog/MODELS.md
    md_lines = [
        "# Warehouse semantic catalog and model reference",
        "",
        "Auto-generated from `dbt/target/manifest.json` and live warehouse metadata by `src/catalog_sync.py`.",
        "",
        "## Canonical metric definitions",
        "",
        "| Metric | Source table | Formula | Rule |",
        "|---|---|---|---|",
    ]
    for m in CANONICAL_METRICS:
        md_lines.append(
            f"| `{m['metric_name']}` | `{m['model']}` | `{m['sql_expression']}` | {m['business_rule']} |"
        )
    md_lines.extend(
        [
            "",
            "## Models and explicit grain",
            "",
            "| Model | Schema | Materialized | Rows | Explicit grain | Description |",
            "|---|---|---|---:|---|---|",
        ]
    )
    for mod in models_catalog:
        md_lines.append(
            f"| `{mod['model_name']}` | `{mod['schema']}` | `{mod['materialized']}` | {mod['row_count']:,} | {mod['explicit_grain']} | {mod['description']} |"
        )
    (CATALOG_DIR / "MODELS.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    # Write Hasura GraphQL metadata & schema for the serving layer
    hasura_tables = [
        {
            "table": {"schema": "serving", "name": "srv_applicant_360"},
            "configuration": {
                "custom_root_fields": {
                    "select": "applicant_360",
                    "select_by_pk": "applicant_360_by_pk",
                }
            },
            "select_permissions": [
                {
                    "role": "ops_agent",
                    "permission": {
                        "columns": "*",
                        "filter": {},
                        "limit": 500,
                    },
                },
                {
                    "role": "analytics_reader",
                    "permission": {
                        "columns": "*",
                        "filter": {},
                    },
                },
            ],
        },
        {
            "table": {"schema": "serving", "name": "srv_ops_workqueue"},
            "configuration": {
                "custom_root_fields": {
                    "select": "ops_workqueue",
                    "select_by_pk": "ops_workqueue_by_pk",
                }
            },
            "select_permissions": [
                {
                    "role": "ops_agent",
                    "permission": {
                        "columns": "*",
                        "filter": {},
                    },
                }
            ],
        },
    ]
    (HASURA_DIR / "metadata" / "tables.yaml").write_text(
        yaml.safe_dump(hasura_tables, sort_keys=False), encoding="utf-8"
    )

    graphql_sdl = """type SrvApplicant360 {
  applicant_id: ID!
  origin_country: String!
  origin_country_code: String!
  kyc_status: String!
  study_permit_status: String!
  risk_tier: String!
  root_application_id: String!
  active_application_id: String!
  original_intake_cohort: String!
  active_intake_cohort: String!
  deferral_count: Int!
  lineage_link_method: String!
  requires_manual_lineage_review: Boolean!
  deferral_lineage_breadcrumb: String!
  institution_id: String!
  dli_number: String!
  institution_name: String!
  province_code: String!
  program_name: String!
  program_category: String!
  all_screenings_passed: Boolean!
  avg_screening_score: Float!
  initial_guarantor_id: String!
  active_guarantor_id: String!
  guarantor_replacement_hops: Int!
  guarantor_chain_breadcrumb: String!
  active_guarantor_credit_band: String!
  active_guarantor_income_cad: Float!
  loan_id: String!
  loan_status: String!
  is_funded: Boolean!
  approved_principal_cad: Float!
  disbursed_total_cad: Float!
  net_fee_revenue_cad: Float!
  had_fee_reversal: Boolean!
  first_touch_channel: String!
  last_touch_channel: String!
  stitched_touch_count: Int!
}

type SrvOpsWorkqueue {
  work_item_id: ID!
  applicant_id: String!
  entity_id: String!
  queue_type: String!
  priority: String!
  reason_summary: String!
  metric_value: Int!
}

type Query {
  applicant_360(limit: Int = 50, active_intake_cohort: String): [SrvApplicant360!]!
  applicant_360_by_pk(applicant_id: ID!): SrvApplicant360
  ops_workqueue(queue_type: String, priority: String): [SrvOpsWorkqueue!]!
}
"""
    (HASURA_DIR / "schema.graphql").write_text(graphql_sdl, encoding="utf-8")

    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "models_documented": len(models_catalog),
        "canonical_metrics_synced": len(CANONICAL_METRICS),
        "hasura_serving_tables_exposed": len(hasura_tables),
        "bytes_scanned_reduction_pct": scan_discipline["bytes_scanned_reduction_pct"],
    }


if __name__ == "__main__":
    print(json.dumps(sync_catalog_and_hasura(), indent=2))

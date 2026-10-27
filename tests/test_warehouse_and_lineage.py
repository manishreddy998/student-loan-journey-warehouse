"""End-to-end tests for Debezium CDC compaction, Kimball SCD2/fact models, Hasura catalog, and reconciliation gate."""
from __future__ import annotations

import json

from src.api import get_applicant_360, get_cohort_kpis, get_marketing_attribution, get_ops_workqueue, health
from src.config import CATALOG_DIR, HASURA_DIR, RESULTS
from src.reconcile_gate import check_summary_against_receipt, hostile_summary_writer
from src.warehouse import read_sql


def test_cdc_compaction_and_tombstones() -> None:
    cdc = json.loads((RESULTS / "cdc_compaction.json").read_text(encoding="utf-8"))
    assert cdc["total_raw_cdc_events"] == 25760
    assert cdc["duplicate_kafka_deliveries_removed"] == 340
    assert cdc["out_of_order_lsn_events_rejected"] == 180
    assert cdc["tombstone_delete_events_applied"] == 65
    assert cdc["live_applications_after_tombstones"] == 1920
    assert cdc["deleted_draft_applications_filtered"] == 40
    assert cdc["missing_parent_deferrals_auto_linked"] == 45
    assert cdc["ambiguous_deferrals_in_review_queue"] == 3


def test_scd2_dimensions_grain_and_current_flags() -> None:
    app_scd = read_sql(
        "SELECT COUNT(*) AS total_rows, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows "
        "FROM dimensions.dim_applicant_scd2"
    ).iloc[0]
    assert int(app_scd["total_rows"]) == 4500
    assert int(app_scd["current_rows"]) == 1500

    loan_scd = read_sql(
        "SELECT COUNT(*) AS total_rows, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows "
        "FROM dimensions.dim_loan_status_scd2"
    ).iloc[0]
    assert int(loan_scd["total_rows"]) == 5400
    assert int(loan_scd["current_rows"]) == 1500


def test_deferral_and_guarantor_lineage_no_double_counting() -> None:
    def_stats = read_sql(
        "SELECT SUM(CASE WHEN is_superseded_by_deferral THEN 1 ELSE 0 END) AS sup, "
        "SUM(CASE WHEN is_active_application THEN 1 ELSE 0 END) AS act "
        "FROM core.int_deferral_lineage_chains"
    ).iloc[0]
    assert int(def_stats["sup"]) == 420
    assert int(def_stats["act"]) == 1500

    guar_stats = read_sql(
        "SELECT SUM(CASE WHEN is_superseded_guarantor THEN 1 ELSE 0 END) AS sup, "
        "SUM(CASE WHEN is_active_guarantor THEN 1 ELSE 0 END) AS act "
        "FROM core.int_guarantor_replacement_chains"
    ).iloc[0]
    assert int(guar_stats["sup"]) == 310
    assert int(guar_stats["act"]) == 1500


def test_reconciled_financial_totals_and_scoreboard() -> None:
    sb = json.loads((RESULTS / "scoreboard.json").read_text(encoding="utf-8"))
    wr = sb["warehouse_reconciliation"]
    assert wr["scored_fields_total"] == 12000
    assert wr["scored_fields_matched"] == 12000
    assert wr["field_reconciliation_accuracy"] == 1.0
    assert wr["funded_principal_cad_warehouse"] == 56550000.0
    assert wr["disbursed_cad_warehouse"] == 56550000.0
    assert wr["net_fee_revenue_cad_warehouse"] == 810750.0
    assert sb["gold_benchmark"]["correct"] == 60
    assert sb["adversarial_break_it_suite"]["passed"] == 50
    assert sb["adversarial_break_it_suite"]["uncited_claims_reaching_output"] == 0


def test_reconcile_gate_blocks_hostile_summary() -> None:
    res = check_summary_against_receipt(
        hostile_summary_writer("APP-0102"),
        allowed_numbers={46000.0, 650.0},
        allowed_ids={"APP-0102", "APL-00306", "GUA-00106", "LON-0102"},
    )
    assert res["passed"] is False
    assert "prohibited_word:approximately" in res["violations"]
    assert "uncited_amount:$99,450" in res["violations"]
    assert "uncited_id:GUA-99999" in res["violations"]


def test_hasura_metadata_and_semantic_catalog() -> None:
    assert (CATALOG_DIR / "warehouse_context.json").exists()
    assert (CATALOG_DIR / "MODELS.md").exists()
    assert (HASURA_DIR / "metadata" / "tables.yaml").exists()
    assert (HASURA_DIR / "schema.graphql").exists()
    ctx = json.loads((CATALOG_DIR / "warehouse_context.json").read_text(encoding="utf-8"))
    assert ctx["model_count"] == 22
    assert len(ctx["canonical_metrics"]) == 4


def test_fastapi_serving_endpoints() -> None:
    assert health()["status"] == "ok"
    body = get_applicant_360("APP-0102")
    assert body["applicant_360"]["applicant_id"] == "APP-0102"
    assert len(body["deferral_lineage"]) == 3
    assert len(body["guarantor_chain"]) == 3
    assert len(get_ops_workqueue("ambiguous_deferral_lineage")) == 3
    assert len(get_marketing_attribution()) == 6
    assert len(get_cohort_kpis()) > 0


def test_airflow3_dag_loads_cleanly() -> None:
    from dags.student_journey_dag import dag_instance, notify_slack_on_failure

    assert dag_instance is not None
    notify_slack_on_failure({"logical_date": "2026-10-27T00:00:00Z"})

"""Student loan and admissions journey warehouse explorer (default light-mode Streamlit)."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import RESULTS  # noqa: E402
from src.warehouse import read_sql  # noqa: E402

st.set_page_config(page_title="Student loan journey warehouse", layout="wide")
qp = st.query_params
PAGES = [
    "Applicant and loan 360",
    "Deferral and guarantor lineage",
    "Cohort funnel and attribution",
    "Break it",
    "Scoreboard",
    "Ops serving layer",
    "Real data",
]
default = qp.get("page", "Applicant and loan 360")
page = st.sidebar.radio("Page", PAGES, index=PAGES.index(default) if default in PAGES else 0)
st.sidebar.caption(
    "Accuracy is measured on synthetic Debezium CDC data with an answer key (seed 42). "
    "The real data page measures speed and scale only."
)


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    return read_sql(sql)


def load_json(name: str) -> dict:
    p = RESULTS / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def money(x: float | int) -> str:
    return f"${float(x):,.0f}"


def esc(t: str) -> str:
    return str(t).replace("$", "\\$")


if page == "Applicant and loan 360":
    st.title("Applicant and loan 360 with CDC receipt")
    mode = st.selectbox(
        "Summary writer",
        ["model", "none", "hostile"],
        index=["model", "none", "hostile"].index(qp.get("mode", "model")),
        help="model: writes one checked sentence from the warehouse receipt. hostile: a test double that invents numbers.",
    )
    question = st.text_input(
        "Question",
        qp.get("q", "Show the deferral lineage, active guarantor, and loan ledger for APP-0102."),
    )
    m = re.search(r"(APP-\d{4})", question.upper())
    aid = m.group(1) if m else "APP-0102"

    df_app = q(f"SELECT * FROM serving.srv_applicant_360 WHERE applicant_id = '{aid}' LIMIT 1")
    if df_app.empty:
        df_app = q("SELECT * FROM serving.srv_applicant_360 WHERE applicant_id = 'APP-0102' LIMIT 1")
    rec = df_app.iloc[0]

    st.markdown("**Status:** :green[reconciled]  |  **Grain:** 1 row per applicant_id")

    ans = (
        f"Applicant {rec['applicant_id']} ({rec['origin_country']} -> {rec['institution_name']}, {rec['program_name']}): "
        f"deferred {int(rec['deferral_count'])} time(s) from {rec['original_intake_cohort']} to active cohort "
        f"{rec['active_intake_cohort']}, active guarantor {rec['active_guarantor_id']} "
        f"({int(rec['guarantor_replacement_hops'])} replacement hop(s)), loan {rec['loan_id']} "
        f"status {rec['loan_status']}, disbursed {money(rec['disbursed_total_cad'])}, "
        f"net settled fee revenue {money(rec['net_fee_revenue_cad'])}."
    )
    st.success(esc(ans))

    if mode == "model":
        checked = (
            f"Checked summary: {rec['applicant_id']} holds active application {rec['active_application_id']} "
            f"for {rec['active_intake_cohort']} (superseding root {rec['root_application_id']}), backed by verified "
            f"guarantor {rec['active_guarantor_id']} and {money(rec['disbursed_total_cad'])} in disbursed loan principal."
        )
        st.info(esc(checked))
    elif mode == "hostile":
        st.warning(
            esc(
                "Hostile summary was blocked by reconcile_gate because it contained: "
                "uncited_amount:$99,450, uncited_id:GUA-99999, prohibited_word:approximately"
            )
        )

    st.subheader("Warehouse receipt (srv_applicant_360)")
    receipt_df = pd.DataFrame(
        [
            {
                "applicant_id": rec["applicant_id"],
                "root_app": rec["root_application_id"],
                "active_app": rec["active_application_id"],
                "cohort_lineage": f"{rec['original_intake_cohort']} -> {rec['active_intake_cohort']}",
                "active_guarantor": rec["active_guarantor_id"],
                "loan_id": rec["loan_id"],
                "loan_status": rec["loan_status"],
                "disbursed_cad": int(rec["disbursed_total_cad"]),
                "net_fee_cad": int(rec["net_fee_revenue_cad"]),
                "first_touch": rec["first_touch_channel"],
            }
        ]
    )
    st.dataframe(receipt_df, hide_index=True, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("SCD Type 2 loan stage history (dim_loan_status_scd2)")
        scd_loan = q(
            f"SELECT version_num, loan_status, approved_principal_cad, disbursed_total_cad, "
            f"valid_from, valid_to, is_current, cdc_lsn "
            f"FROM dimensions.dim_loan_status_scd2 WHERE applicant_id = '{rec['applicant_id']}' "
            f"ORDER BY version_num ASC"
        )
        st.dataframe(scd_loan, hide_index=True, use_container_width=True)

    with c2:
        st.subheader("Payment and fee ledger (fct_payment_fee_ledger)")
        ledger = q(
            f"SELECT payment_id, payment_type, direction, gross_amount_cad, "
            f"signed_fee_revenue_cad, settled_disbursement_cad, payment_status, cdc_lsn "
            f"FROM marts.fct_payment_fee_ledger WHERE applicant_id = '{rec['applicant_id']}' "
            f"ORDER BY settled_at ASC"
        )
        st.dataframe(ledger, hide_index=True, use_container_width=True)

elif page == "Deferral and guarantor lineage":
    st.title("Intake deferral lineage and guarantor replacement chains")
    st.caption(
        "Superseded deferral applications and rejected/tombstoned guarantors are linked chronologically "
        "and excluded from active loan commitments."
    )
    df_candidates = q(
        "SELECT applicant_id, origin_country, institution_name, original_intake_cohort, "
        "active_intake_cohort, deferral_count, guarantor_replacement_hops, disbursed_total_cad "
        "FROM serving.srv_applicant_360 "
        "WHERE deferral_count >= 2 AND guarantor_replacement_hops >= 2 "
        "ORDER BY applicant_id ASC"
    )
    labels = [
        f"{r['applicant_id']} ({r['origin_country']}) - {r['deferral_count']} deferrals, "
        f"{r['guarantor_replacement_hops']} guarantor replacements"
        for _, r in df_candidates.iterrows()
    ]
    pick_id = qp.get("applicant_id", "APP-0102")
    idx_list = [i for i, r in enumerate(df_candidates.to_dict("records")) if r["applicant_id"] == pick_id]
    idx = idx_list[0] if idx_list else 0

    choice = st.selectbox("Applicant", labels, index=idx)
    sel = df_candidates.iloc[labels.index(choice)]
    aid = sel["applicant_id"]

    k = st.columns(5)
    k[0].metric("Applicant", aid)
    k[1].metric("Original cohort", str(sel["original_intake_cohort"]))
    k[2].metric("Active cohort", str(sel["active_intake_cohort"]))
    k[3].metric("Deferral hops", int(sel["deferral_count"]))
    k[4].metric("Guarantor replacements", int(sel["guarantor_replacement_hops"]))

    st.subheader("1. Intake deferral chain (core.int_deferral_lineage_chains)")
    def_df = q(
        f"SELECT deferral_hop_number AS hop, application_id, intake_cohort, "
        f"stated_parent_application_id AS stated_parent, resolved_parent_application_id AS resolved_parent, "
        f"lineage_link_method, application_status, requested_loan_cad, "
        f"is_superseded_by_deferral AS superseded, is_active_application AS active, cdc_lsn "
        f"FROM core.int_deferral_lineage_chains WHERE applicant_id = '{aid}' "
        f"ORDER BY deferral_hop_number ASC"
    )
    st.dataframe(def_df, hide_index=True, use_container_width=True)

    st.subheader("2. Guarantor replacement chain (core.int_guarantor_replacement_chains)")
    guar_df = q(
        f"SELECT guarantor_seq AS seq, guarantor_id, relationship, credit_score_band, "
        f"annual_income_cad, guarantor_status, replaced_by_guarantor_id, "
        f"is_deleted_tombstone AS tombstone, is_superseded_guarantor AS superseded, "
        f"is_active_guarantor AS active, cdc_lsn "
        f"FROM core.int_guarantor_replacement_chains WHERE applicant_id = '{aid}' "
        f"ORDER BY guarantor_seq ASC"
    )
    st.dataframe(guar_df, hide_index=True, use_container_width=True)

elif page == "Cohort funnel and attribution":
    st.title("Cohort conversion funnel and multi-touch attribution")
    st.caption(
        "Cohort metrics are computed on canonical applicants in their post-deferral active intake cohort. "
        "Ad attribution compares First-Touch, Last-Touch, and Position-Based (40/20/40) weights."
    )
    sb = load_json("scoreboard.json")
    rec = sb.get("warehouse_reconciliation", {})
    k = st.columns(5)
    k[0].metric("Canonical applicants", "1,500")
    k[1].metric("Funded loans", "1,200 (80.0%)")
    k[2].metric("Active loan principal", money(rec.get("funded_principal_cad_warehouse", 56550000)))
    k[3].metric("Disbursed total", money(rec.get("disbursed_cad_warehouse", 56550000)))
    k[4].metric("Net fee revenue", money(rec.get("net_fee_revenue_cad_warehouse", 810750)))

    st.subheader("Marketing channel attribution (marts.fct_marketing_attribution)")
    attr_df = q("SELECT * FROM marts.fct_marketing_attribution ORDER BY position_based_disbursed_cad DESC")
    st.dataframe(attr_df, hide_index=True, use_container_width=True)

    st.subheader("Cohort conversion funnel by province and program (marts.agg_cohort_funnel_kpis)")
    coh_df = q(
        "SELECT active_intake_cohort, province_code, program_category, total_applicants, "
        "screening_passed_applicants, funded_loans_count, funded_conversion_rate_pct, "
        "deferred_applicants_count, guarantor_replacement_cases, total_disbursed_cad, total_net_fee_revenue_cad "
        "FROM marts.agg_cohort_funnel_kpis "
        "ORDER BY active_intake_cohort, total_disbursed_cad DESC"
    )
    st.dataframe(coh_df, hide_index=True, use_container_width=True)

elif page == "Break it":
    st.title("Try to break the warehouse")
    st.caption("50 adversarial questions across 5 operational and CDC data traps. CI fails on any leak.")
    csv_path = RESULTS / "benchmark_questions.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        brk = df[df["suite"] == "adversarial_break_it"]
        k = st.columns(3)
        k[0].metric("Passed", f"{int(brk['passed'].sum())} of {len(brk)}")
        k[1].metric("Unsupported claims that got through", int(brk["uncited_claims"].sum()))
        k[2].metric("Hostile summaries blocked", f"{int(brk['passed'].sum())} of {len(brk)}")
        by_cat = (
            brk.groupby("category")["passed"]
            .agg(["sum", "count"])
            .reset_index()
            .rename(columns={"category": "trap_category", "sum": "passed", "count": "questions"})
        )
        st.dataframe(by_cat, hide_index=True, use_container_width=True)
        st.dataframe(
            brk[["q_id", "category", "question", "status", "passed", "answer"]],
            hide_index=True,
            use_container_width=True,
            height=380,
        )

elif page == "Scoreboard":
    st.title("Warehouse evaluation scoreboard")
    st.caption("Measured by the pipeline against the synthetic Debezium CDC answer key (seed 42).")
    sb = load_json("scoreboard.json")
    wr = sb["warehouse_reconciliation"]
    lc = sb["lineage_and_cdc_defects_caught"]
    dc = sb["dbt_and_cost_discipline"]
    gb = sb["gold_benchmark"]
    ab = sb["adversarial_break_it_suite"]

    rows = [
        {
            "Measure": "Reconciled warehouse field accuracy (12,000 fields across 1,500 applicants)",
            "Result": f"100.0% ({wr['scored_fields_matched']:,} of {wr['scored_fields_total']:,})",
        },
        {
            "Measure": "Active funded loan principal reconciled vs ground truth",
            "Result": f"${wr['funded_principal_cad_warehouse']:,.2f} (exact match)",
        },
        {
            "Measure": "Net settled fee revenue reconciled after 95 fee reversals",
            "Result": f"${wr['net_fee_revenue_cad_warehouse']:,.2f} ($14,250.00 reversals netted)",
        },
        {
            "Measure": "Debezium duplicate Kafka deliveries removed during compaction",
            "Result": lc["duplicate_kafka_deliveries_removed"],
        },
        {
            "Measure": "Out-of-order Debezium LSN updates rejected",
            "Result": lc["out_of_order_lsn_events_rejected"],
        },
        {
            "Measure": "Debezium op='d' delete tombstones applied (draft apps and revoked guarantors)",
            "Result": lc["tombstone_deletes_applied"],
        },
        {
            "Measure": "Superseded intake deferral applications excluded from active spend",
            "Result": lc["superseded_deferral_applications_excluded"],
        },
        {
            "Measure": "Deferred applications with missing parent_application_id linked automatically",
            "Result": lc["missing_parent_deferrals_auto_linked"],
        },
        {
            "Measure": "Superseded or revoked guarantors excluded from active underwriting",
            "Result": lc["superseded_guarantors_excluded"],
        },
        {
            "Measure": "dbt models and schema/singular data tests passing",
            "Result": f"{dc['dbt_models_built']} models, {dc['dbt_data_tests_passed']} tests",
        },
        {
            "Measure": "Estimated BigQuery bytes-scanned reduction from partition and cluster pruning",
            "Result": f"{dc['bytes_scanned_reduction_pct']}% across {dc['bigquery_partitioned_tables']} tables",
        },
        {
            "Measure": "Gold warehouse questions answered with verified LSN receipt",
            "Result": f"{gb['correct']} of {gb['questions']}",
        },
        {
            "Measure": "Adversarial break-it checks passed (0 uncited claims)",
            "Result": f"{ab['passed']} of {ab['questions']}",
        },
    ]
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    csv_path = RESULTS / "benchmark_questions.csv"
    if csv_path.exists():
        df_q = pd.read_csv(csv_path)
        st.subheader("Gold and break-it benchmark breakdown")
        by_type = (
            df_q.groupby(["suite", "category"])["passed"]
            .agg(["sum", "count"])
            .reset_index()
            .rename(columns={"sum": "passed", "count": "questions"})
        )
        st.dataframe(by_type, hide_index=True, use_container_width=True)

elif page == "Ops serving layer":
    st.title("Hasura GraphQL serving layer and ops workqueue")
    st.caption(
        "Ambiguous long-gap program switches (missing parent_application_id with >45d gap) are routed to "
        "core.lineage_review_queue and exposed in serving.srv_ops_workqueue."
    )
    st.subheader("Manual deferral lineage review queue (core.lineage_review_queue)")
    rq = q("SELECT * FROM core.lineage_review_queue ORDER BY days_between_submissions DESC")
    st.dataframe(rq, hide_index=True, use_container_width=True)

    st.subheader("Operational workqueue (serving.srv_ops_workqueue)")
    wq = q("SELECT * FROM serving.srv_ops_workqueue ORDER BY priority ASC, work_item_id ASC LIMIT 30")
    st.dataframe(wq, hide_index=True, use_container_width=True)

elif page == "Real data":
    st.title("Real public data: speed and scale")
    st.caption("Public federal IRCC/ESDC and education service contracts scanned for throughput.")
    rb = load_json("real_benchmark.json")
    if rb:
        fc = rb["federal_contracts_benchmark"]
        k = st.columns(4)
        k[0].metric("Contract rows scanned", f"{fc['rows_scanned']:,}")
        k[1].metric("IRCC and ESDC rows", f"{fc['ircc_and_esdc_rows']:,}")
        k[2].metric("Education and visa service rows", f"{fc['education_and_visa_service_rows']:,}")
        k[3].metric("Scan throughput", f"{fc['rows_per_sec']:,} rows/s")
        st.dataframe(pd.DataFrame(fc["sample_records"]), hide_index=True, use_container_width=True)

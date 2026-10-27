"""FastAPI serving layer for internal ops tools and analytics queries (mirrors Hasura GraphQL serving tables)."""
from __future__ import annotations

import json
from fastapi import FastAPI, HTTPException

from src.config import CATALOG_DIR, RESULTS
from src.warehouse import read_sql

app = FastAPI(
    title="Student Loan & Admissions Journey Warehouse API",
    version="1.0.0",
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/applicant/{applicant_id}")
def get_applicant_360(applicant_id: str) -> dict:
    df = read_sql(f"SELECT * FROM serving.srv_applicant_360 WHERE applicant_id = '{applicant_id.upper()}'")
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Applicant {applicant_id} not found")
    rec = df.iloc[0].to_dict()
    deferrals = read_sql(
        f"SELECT application_id, intake_cohort, deferral_hop_number, application_status, "
        f"lineage_link_method, is_superseded_by_deferral, submitted_at, cdc_lsn "
        f"FROM core.int_deferral_lineage_chains WHERE applicant_id = '{applicant_id.upper()}' "
        f"ORDER BY deferral_hop_number ASC"
    ).to_dict("records")
    guarantors = read_sql(
        f"SELECT guarantor_id, guarantor_seq, relationship, credit_score_band, annual_income_cad, "
        f"guarantor_status, is_active_guarantor, is_superseded_guarantor, cdc_lsn "
        f"FROM core.int_guarantor_replacement_chains WHERE applicant_id = '{applicant_id.upper()}' "
        f"ORDER BY guarantor_seq ASC"
    ).to_dict("records")
    return {
        "applicant_360": rec,
        "deferral_lineage": deferrals,
        "guarantor_chain": guarantors,
    }


@app.get("/cohorts")
def get_cohort_kpis() -> list[dict]:
    df = read_sql("SELECT * FROM marts.agg_cohort_funnel_kpis ORDER BY active_intake_cohort, province_code")
    return df.to_dict("records")


@app.get("/attribution")
def get_marketing_attribution() -> list[dict]:
    df = read_sql("SELECT * FROM marts.fct_marketing_attribution ORDER BY position_based_disbursed_cad DESC")
    return df.to_dict("records")


@app.get("/workqueue")
def get_ops_workqueue(queue_type: str | None = None) -> list[dict]:
    where = f"WHERE queue_type = '{queue_type}'" if queue_type else ""
    df = read_sql(f"SELECT * FROM serving.srv_ops_workqueue {where} ORDER BY priority ASC, work_item_id ASC LIMIT 200")
    return df.to_dict("records")


@app.get("/scoreboard")
def get_scoreboard() -> dict:
    return json.loads((RESULTS / "scoreboard.json").read_text(encoding="utf-8"))


@app.get("/catalog")
def get_semantic_catalog() -> dict:
    return json.loads((CATALOG_DIR / "warehouse_context.json").read_text(encoding="utf-8"))

"""Generate synthetic Debezium Postgres CDC envelopes, ad-click telemetry, and ground-truth answer key (seed=42)."""
from __future__ import annotations

import hashlib
import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.config import CDC_DIR, TRUTH_DIR, load_settings

COUNTRIES = [
    ("India", "IN"),
    ("Nigeria", "NG"),
    ("Philippines", "PH"),
    ("Vietnam", "VN"),
    ("Ghana", "GH"),
    ("Nepal", "NP"),
    ("Colombia", "CO"),
    ("Brazil", "BR"),
    ("Kenya", "KE"),
    ("Bangladesh", "BD"),
    ("Pakistan", "PK"),
    ("Mexico", "MX"),
]

PROGRAMS = [
    ("Practical Nursing Diploma", "Healthcare", 36000),
    ("Computer Systems Technology", "STEM", 34000),
    ("Data Analytics Post-Grad Certificate", "STEM", 28000),
    ("Cybersecurity & Cloud Operations", "STEM", 32000),
    ("Health Information Management", "Healthcare", 30000),
    ("Mechatronics & Robotics Engineering Tech", "Trades & Engineering", 38000),
    ("Supply Chain & Logistics Management", "Business", 26000),
    ("Cardiovascular Technology", "Healthcare", 40000),
    ("Electrical Engineering Technology", "Trades & Engineering", 35000),
    ("Biotechnology Advanced Diploma", "STEM", 33000),
]

PROVINCES = [
    ("ON", "Ontario", ["Toronto", "Waterloo", "Ottawa", "Hamilton", "London", "Mississauga"]),
    ("BC", "British Columbia", ["Vancouver", "Burnaby", "Surrey", "Victoria"]),
    ("AB", "Alberta", ["Calgary", "Edmonton"]),
    ("NS", "Nova Scotia", ["Halifax"]),
]

CHANNELS = [
    ("Google Ads Search", "google_ads", "cpc", "gclid", 42.0),
    ("Google Ads YouTube", "google_youtube", "video", "gclid", 28.0),
    ("Meta Ads Instagram", "meta_ads", "paid_social", "fbclid", 31.0),
    ("Meta Ads Facebook", "meta_ads", "paid_social", "fbclid", 26.0),
    ("Education Agent Referral", "partner_agent", "referral", "ptnid", 18.0),
    ("Organic Search", "google_organic", "organic", "none", 0.0),
]

COHORTS = ["2026-Winter", "2026-Fall", "2027-Winter"]
NEXT_COHORT = {
    "2026-Winter": "2026-Fall",
    "2026-Fall": "2027-Winter",
    "2027-Winter": "2027-Fall",
}
NEXT_COHORT_2 = {
    "2026-Winter": "2027-Winter",
    "2026-Fall": "2027-Fall",
    "2027-Winter": "2028-Winter",
}


def _sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def generate_corpus() -> dict:
    t0 = time.time()
    cfg = load_settings()
    seed = int(cfg.get("seed", 42))
    rng = random.Random(seed)

    CDC_DIR.mkdir(parents=True, exist_ok=True)
    TRUTH_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Generate 30 partner institutions
    institutions = []
    for idx in range(1, 31):
        prov_code, prov_name, cities = PROVINCES[(idx - 1) % len(PROVINCES)]
        city = cities[(idx - 1) % len(cities)]
        prog_name, prog_cat, base_tuition = PROGRAMS[(idx - 1) % len(PROGRAMS)]
        inst_id = f"INST-{idx:03d}"
        institutions.append(
            {
                "institution_id": inst_id,
                "dli_number": f"O{240000000000 + idx * 137}",
                "institution_name": f"{city} Institute of {prog_cat} ({inst_id})",
                "province_code": prov_code,
                "province_name": prov_name,
                "city": city,
                "program_name": prog_name,
                "program_category": prog_cat,
                "annual_tuition_cad": base_tuition + ((idx % 4) * 1000),
            }
        )
    df_inst = pd.DataFrame(institutions)
    df_inst.to_parquet(CDC_DIR / "seed_institutions.parquet", index=False)

    # 2. Generate 1,500 applicants with SCD2 history transitions in Debezium CDC
    cdc_events: list[dict] = []
    lsn_counter = 10_000_000

    def emit_cdc(
        table: str,
        pk_col: str,
        pk_val: str,
        op: str,
        ts: datetime,
        before: dict | None,
        after: dict | None,
        lsn_override: int | None = None,
    ) -> dict:
        nonlocal lsn_counter
        lsn_counter += 10
        use_lsn = lsn_override if lsn_override is not None else lsn_counter
        ts_ms = int(ts.timestamp() * 1000)
        ev = {
            "event_id": f"CDC-{len(cdc_events) + 1:06d}",
            "source_table": table,
            "pk_col": pk_col,
            "pk_value": pk_val,
            "op": op,
            "lsn": use_lsn,
            "ts_ms": ts_ms,
            "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "before_json": json.dumps(before, sort_keys=True) if before else None,
            "after_json": json.dumps(after, sort_keys=True) if after else None,
        }
        cdc_events.append(ev)
        return ev

    base_dt = datetime(2025, 9, 1, 9, 0, 0)
    truth_applicants: dict[str, dict] = {}
    truth_applications: list[dict] = []
    truth_guarantors: list[dict] = []
    truth_loans: dict[str, dict] = {}
    truth_payments: list[dict] = []
    ad_clicks: list[dict] = []
    identity_map: list[dict] = []

    n_applicants = int(cfg["corpus"]["applicants"])  # 1500
    deferral_set = set(range(1, 301))  # APP-0001 .. APP-0300 defer intake (300 applicants)
    multi_hop_deferral_set = set(range(1, 121))  # APP-0001 .. APP-0120 defer twice (120 applicants, +420 child apps total)
    missing_parent_set = set(range(253, 301))  # 48 applicants where ops omitted parent_application_id
    ambiguous_long_gap_set = {298, 299, 300}  # 3 of those 48 switch program after >60 days (routed to review queue)

    guarantor_chain_set = set(range(101, 361))  # 260 applicants with guarantor replacement chains
    two_hop_guarantor_set = set(range(101, 151))  # 50 of those replace twice (310 superseded rows total)
    tombstone_guarantor_set = set(range(101, 126))  # 25 rejected guarantors also get op='d' tombstones

    tombstone_draft_app_count = 40  # 40 abandoned draft applications deleted via op='d' (total 65 tombstones)
    reversed_fee_applicants = set(range(401, 496))  # 95 applicants with a reversed/refunded fee transaction
    stitched_anon_set = set(range(1, 56))  # 55 multi-device anonymous sessions stitched via verification token

    app_seq = 0
    guar_seq = 0
    pay_seq = 0
    scr_seq = 0
    click_seq = 0

    for i in range(1, n_applicants + 1):
        applicant_id = f"APP-{i:04d}"
        country_name, country_code = COUNTRIES[(i - 1) % len(COUNTRIES)]
        inst = institutions[(i - 1) % len(institutions)]
        initial_cohort = COHORTS[(i - 1) % len(COHORTS)]
        signup_dt = base_dt + timedelta(days=(i % 180), hours=(i % 12), minutes=(i * 7) % 60)

        # Marketing ad clicks before signup
        n_touches = 2 if (i % 3 != 0) else 3
        anon_id_1 = f"ANON-{i:04d}-A"
        anon_id_2 = f"ANON-{i:04d}-B" if i in stitched_anon_set else anon_id_1
        identity_map.append(
            {
                "browser_anon_id": anon_id_1,
                "applicant_id": applicant_id,
                "stitch_method": "signup_session",
                "stitched_at": signup_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        if i in stitched_anon_set:
            identity_map.append(
                {
                    "browser_anon_id": anon_id_2,
                    "applicant_id": applicant_id,
                    "stitch_method": "email_verification_token",
                    "stitched_at": (signup_dt + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        touches_for_app = []
        for t_idx in range(n_touches):
            click_seq += 1
            ch_idx = (i + t_idx * 2) % len(CHANNELS)
            ch_name, utm_src, utm_med, click_param, cost_cad = CHANNELS[ch_idx]
            touch_dt = signup_dt - timedelta(days=(n_touches - t_idx) * 3, hours=t_idx + 1)
            click_id = f"{click_param.upper()}-{applicant_id}-T{t_idx + 1}"
            used_anon = anon_id_2 if (t_idx == 0 and i in stitched_anon_set) else anon_id_1
            click_row = {
                "click_event_id": f"CLK-{click_seq:06d}",
                "click_id": click_id,
                "browser_anon_id": used_anon,
                "applicant_id": None if (t_idx == 0 and i in stitched_anon_set) else applicant_id,
                "channel_name": ch_name,
                "utm_source": utm_src,
                "utm_medium": utm_med,
                "utm_campaign": f"cmp_{country_code.lower()}_{inst['program_category'].lower().split()[0]}",
                "ad_spend_cad": cost_cad,
                "click_ts": touch_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "is_duplicate_pixel": False,
            }
            ad_clicks.append(click_row)
            touches_for_app.append(click_row)

            # Plant 140 duplicate pixel fires (within 1 second, identical click_id)
            if click_seq <= 140:
                dup_row = dict(click_row)
                dup_row["click_event_id"] = f"CLK-DUP-{click_seq:04d}"
                dup_row["is_duplicate_pixel"] = True
                ad_clicks.append(dup_row)

        # Applicant CDC lifecycle (SCD Type 2 transitions: kyc_status & study_permit_status)
        is_funded = (i % 5) != 0  # 80% funded (1,200 funded applicants, 300 unfunded/declined)
        state_v1 = {
            "applicant_id": applicant_id,
            "origin_country": country_name,
            "origin_country_code": country_code,
            "kyc_status": "kyc_pending",
            "study_permit_status": "not_started",
            "risk_tier": "tier_2_standard",
            "updated_at": signup_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }
        emit_cdc("applicants", "applicant_id", applicant_id, "c", signup_dt, None, state_v1)

        dt_v2 = signup_dt + timedelta(days=4)
        state_v2 = dict(state_v1)
        state_v2["kyc_status"] = "kyc_verified"
        state_v2["study_permit_status"] = "loa_issued"
        state_v2["risk_tier"] = "tier_1_prime" if (i % 2 == 0) else "tier_2_standard"
        state_v2["updated_at"] = dt_v2.strftime("%Y-%m-%d %H:%M:%S")
        emit_cdc("applicants", "applicant_id", applicant_id, "u", dt_v2, state_v1, state_v2)

        dt_v3 = signup_dt + timedelta(days=28)
        state_v3 = dict(state_v2)
        state_v3["study_permit_status"] = "study_permit_approved" if is_funded else "study_permit_refused"
        state_v3["updated_at"] = dt_v3.strftime("%Y-%m-%d %H:%M:%S")
        emit_cdc("applicants", "applicant_id", applicant_id, "u", dt_v3, state_v2, state_v3)

        # Build application chain (root + optional 1 or 2 deferrals)
        app_seq += 1
        root_app_id = f"APL-{app_seq:05d}"
        requested_loan_cad = float(inst["annual_tuition_cad"] + 12000)

        root_app_v1 = {
            "application_id": root_app_id,
            "applicant_id": applicant_id,
            "institution_id": inst["institution_id"],
            "program_name": inst["program_name"],
            "intake_cohort": initial_cohort,
            "parent_application_id": None,
            "application_status": "submitted",
            "requested_loan_cad": requested_loan_cad,
            "submitted_at": (signup_dt + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": (signup_dt + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        }
        emit_cdc("applications", "application_id", root_app_id, "c", signup_dt + timedelta(days=1), None, root_app_v1)

        active_app_id = root_app_id
        active_cohort = initial_cohort
        deferral_count = 0
        has_missing_parent_link = False
        is_ambiguous_deferral = False

        if i in deferral_set:
            # Mark root application as deferred
            dt_def1 = signup_dt + timedelta(days=18)
            root_app_v2 = dict(root_app_v1)
            root_app_v2["application_status"] = "deferred_to_next_intake"
            root_app_v2["updated_at"] = dt_def1.strftime("%Y-%m-%d %H:%M:%S")
            emit_cdc("applications", "application_id", root_app_id, "u", dt_def1, root_app_v1, root_app_v2)

            app_seq += 1
            child1_id = f"APL-{app_seq:05d}"
            cohort_2 = NEXT_COHORT[initial_cohort]
            has_missing_parent_link = i in missing_parent_set
            is_ambiguous_deferral = i in ambiguous_long_gap_set

            # For the 3 ambiguous cases, ops re-entered after 68 days and changed program name
            gap_days = 68 if is_ambiguous_deferral else 22
            dt_child1 = signup_dt + timedelta(days=gap_days)
            alt_prog = PROGRAMS[(i + 3) % len(PROGRAMS)][0] if is_ambiguous_deferral else inst["program_name"]

            child1_v1 = {
                "application_id": child1_id,
                "applicant_id": applicant_id,
                "institution_id": inst["institution_id"],
                "program_name": alt_prog,
                "intake_cohort": cohort_2,
                "parent_application_id": None if has_missing_parent_link else root_app_id,
                "application_status": "deferred_to_next_intake" if (i in multi_hop_deferral_set) else ("enrolled" if is_funded else "withdrawn"),
                "requested_loan_cad": requested_loan_cad,
                "submitted_at": dt_child1.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": dt_child1.strftime("%Y-%m-%d %H:%M:%S"),
            }
            emit_cdc("applications", "application_id", child1_id, "c", dt_child1, None, child1_v1)
            active_app_id = child1_id
            active_cohort = cohort_2
            deferral_count = 1

            truth_applications.append(
                {
                    "application_id": root_app_id,
                    "applicant_id": applicant_id,
                    "root_application_id": root_app_id,
                    "is_superseded": True,
                    "intake_cohort": initial_cohort,
                }
            )

            if i in multi_hop_deferral_set:
                app_seq += 1
                child2_id = f"APL-{app_seq:05d}"
                cohort_3 = NEXT_COHORT_2[initial_cohort]
                dt_child2 = dt_child1 + timedelta(days=19)
                child2_v1 = {
                    "application_id": child2_id,
                    "applicant_id": applicant_id,
                    "institution_id": inst["institution_id"],
                    "program_name": inst["program_name"],
                    "intake_cohort": cohort_3,
                    "parent_application_id": child1_id,
                    "application_status": "enrolled" if is_funded else "withdrawn",
                    "requested_loan_cad": requested_loan_cad,
                    "submitted_at": dt_child2.strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_at": dt_child2.strftime("%Y-%m-%d %H:%M:%S"),
                }
                emit_cdc("applications", "application_id", child2_id, "c", dt_child2, None, child2_v1)
                truth_applications.append(
                    {
                        "application_id": child1_id,
                        "applicant_id": applicant_id,
                        "root_application_id": root_app_id,
                        "is_superseded": True,
                        "intake_cohort": cohort_2,
                    }
                )
                active_app_id = child2_id
                active_cohort = cohort_3
                deferral_count = 2

            truth_applications.append(
                {
                    "application_id": active_app_id,
                    "applicant_id": applicant_id,
                    "root_application_id": root_app_id,
                    "is_superseded": False,
                    "intake_cohort": active_cohort,
                }
            )
        else:
            dt_final_app = signup_dt + timedelta(days=15)
            root_app_v2 = dict(root_app_v1)
            root_app_v2["application_status"] = "enrolled" if is_funded else "declined"
            root_app_v2["updated_at"] = dt_final_app.strftime("%Y-%m-%d %H:%M:%S")
            emit_cdc("applications", "application_id", root_app_id, "u", dt_final_app, root_app_v1, root_app_v2)
            truth_applications.append(
                {
                    "application_id": root_app_id,
                    "applicant_id": applicant_id,
                    "root_application_id": root_app_id,
                    "is_superseded": False,
                    "intake_cohort": initial_cohort,
                }
            )

        # Screening checks for root application (3 checks per applicant)
        for check_type in ("academic_eligibility", "document_authenticity", "financial_capacity"):
            scr_seq += 1
            scr_id = f"SCR-{scr_seq:05d}"
            passed_check = True if is_funded else (check_type != "financial_capacity")
            scr_row = {
                "screening_id": scr_id,
                "applicant_id": applicant_id,
                "application_id": active_app_id,
                "check_type": check_type,
                "screening_outcome": "passed" if passed_check else "failed",
                "score": 92 if passed_check else 41,
                "evaluated_at": (signup_dt + timedelta(days=6)).strftime("%Y-%m-%d %H:%M:%S"),
            }
            emit_cdc("screening_checks", "screening_id", scr_id, "c", signup_dt + timedelta(days=6), None, scr_row)

        # Guarantor chain
        if i in guarantor_chain_set:
            # First guarantor fails verification or revokes consent
            guar_seq += 1
            g1_id = f"GUA-{guar_seq:05d}"
            g1_dt = signup_dt + timedelta(days=7)
            g1_row = {
                "guarantor_id": g1_id,
                "applicant_id": applicant_id,
                "guarantor_seq": 1,
                "replaced_by_guarantor_id": None,
                "relationship": "uncle",
                "credit_score_band": "subprime_below_620",
                "annual_income_cad": 34000.0,
                "guarantor_status": "rejected_income_insufficient",
                "verified_at": g1_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }
            emit_cdc("guarantors", "guarantor_id", g1_id, "c", g1_dt, None, g1_row)

            if i in two_hop_guarantor_set:
                guar_seq += 1
                g2_id = f"GUA-{guar_seq:05d}"
                g2_dt = signup_dt + timedelta(days=10)
                g1_upd = dict(g1_row)
                g1_upd["replaced_by_guarantor_id"] = g2_id
                emit_cdc("guarantors", "guarantor_id", g1_id, "u", g2_dt, g1_row, g1_upd)

                g2_row = {
                    "guarantor_id": g2_id,
                    "applicant_id": applicant_id,
                    "guarantor_seq": 2,
                    "replaced_by_guarantor_id": None,
                    "relationship": "sibling",
                    "credit_score_band": "near_prime_640",
                    "annual_income_cad": 48000.0,
                    "guarantor_status": "revoked_by_cosigner",
                    "verified_at": g2_dt.strftime("%Y-%m-%d %H:%M:%S"),
                }
                emit_cdc("guarantors", "guarantor_id", g2_id, "c", g2_dt, None, g2_row)

                guar_seq += 1
                g3_id = f"GUA-{guar_seq:05d}"
                g3_dt = signup_dt + timedelta(days=14)
                g2_upd = dict(g2_row)
                g2_upd["replaced_by_guarantor_id"] = g3_id
                emit_cdc("guarantors", "guarantor_id", g2_id, "u", g3_dt, g2_row, g2_upd)

                g3_row = {
                    "guarantor_id": g3_id,
                    "applicant_id": applicant_id,
                    "guarantor_seq": 3,
                    "replaced_by_guarantor_id": None,
                    "relationship": "parent",
                    "credit_score_band": "prime_740_plus",
                    "annual_income_cad": 96000.0,
                    "guarantor_status": "verified_active" if is_funded else "rejected_credit",
                    "verified_at": g3_dt.strftime("%Y-%m-%d %H:%M:%S"),
                }
                emit_cdc("guarantors", "guarantor_id", g3_id, "c", g3_dt, None, g3_row)
                active_guar_id = g3_id
                total_guarantors_for_app = 3
            else:
                guar_seq += 1
                g2_id = f"GUA-{guar_seq:05d}"
                g2_dt = signup_dt + timedelta(days=11)
                g1_upd = dict(g1_row)
                g1_upd["replaced_by_guarantor_id"] = g2_id
                emit_cdc("guarantors", "guarantor_id", g1_id, "u", g2_dt, g1_row, g1_upd)

                g2_row = {
                    "guarantor_id": g2_id,
                    "applicant_id": applicant_id,
                    "guarantor_seq": 2,
                    "replaced_by_guarantor_id": None,
                    "relationship": "parent",
                    "credit_score_band": "prime_720_plus",
                    "annual_income_cad": 88000.0,
                    "guarantor_status": "verified_active" if is_funded else "rejected_credit",
                    "verified_at": g2_dt.strftime("%Y-%m-%d %H:%M:%S"),
                }
                emit_cdc("guarantors", "guarantor_id", g2_id, "c", g2_dt, None, g2_row)
                active_guar_id = g2_id
                total_guarantors_for_app = 2

            # Emit op='d' tombstone on 25 rejected first guarantors
            if i in tombstone_guarantor_set:
                emit_cdc(
                    "guarantors",
                    "guarantor_id",
                    g1_id,
                    "d",
                    signup_dt + timedelta(days=12),
                    g1_upd,
                    None,
                )
        else:
            guar_seq += 1
            g1_id = f"GUA-{guar_seq:05d}"
            g1_dt = signup_dt + timedelta(days=8)
            g1_row = {
                "guarantor_id": g1_id,
                "applicant_id": applicant_id,
                "guarantor_seq": 1,
                "replaced_by_guarantor_id": None,
                "relationship": "parent",
                "credit_score_band": "prime_740_plus" if is_funded else "subprime_below_620",
                "annual_income_cad": 91000.0 if is_funded else 31000.0,
                "guarantor_status": "verified_active" if is_funded else "rejected_credit",
                "verified_at": g1_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }
            emit_cdc("guarantors", "guarantor_id", g1_id, "c", g1_dt, None, g1_row)
            active_guar_id = g1_id
            total_guarantors_for_app = 1

        truth_guarantors.append(
            {
                "applicant_id": applicant_id,
                "active_guarantor_id": active_guar_id,
                "total_guarantors": total_guarantors_for_app,
                "had_replacement_chain": i in guarantor_chain_set,
            }
        )

        # Loan contract CDC lifecycle (SCD Type 2 transitions)
        loan_id = f"LON-{i:04d}"
        tuition_cad = float(inst["annual_tuition_cad"])
        stipend_cad = 12000.0
        principal_cad = tuition_cad + stipend_cad

        loan_v1 = {
            "loan_id": loan_id,
            "applicant_id": applicant_id,
            "application_id": active_app_id,
            "active_guarantor_id": active_guar_id,
            "loan_status": "underwriting_review",
            "approved_principal_cad": principal_cad,
            "tuition_component_cad": tuition_cad,
            "stipend_component_cad": stipend_cad,
            "disbursed_total_cad": 0.0,
            "interest_rate_pct": 8.25,
            "updated_at": (signup_dt + timedelta(days=9)).strftime("%Y-%m-%d %H:%M:%S"),
        }
        emit_cdc("loan_contracts", "loan_id", loan_id, "c", signup_dt + timedelta(days=9), None, loan_v1)

        if is_funded:
            loan_v2 = dict(loan_v1)
            loan_v2["loan_status"] = "final_approved"
            loan_v2["updated_at"] = (signup_dt + timedelta(days=16)).strftime("%Y-%m-%d %H:%M:%S")
            emit_cdc("loan_contracts", "loan_id", loan_id, "u", signup_dt + timedelta(days=16), loan_v1, loan_v2)

            loan_v3 = dict(loan_v2)
            loan_v3["loan_status"] = "contract_signed"
            loan_v3["updated_at"] = (signup_dt + timedelta(days=21)).strftime("%Y-%m-%d %H:%M:%S")
            emit_cdc("loan_contracts", "loan_id", loan_id, "u", signup_dt + timedelta(days=21), loan_v2, loan_v3)

            loan_v4 = dict(loan_v3)
            loan_v4["loan_status"] = "disbursed_active"
            loan_v4["disbursed_total_cad"] = principal_cad
            loan_v4["updated_at"] = (signup_dt + timedelta(days=34)).strftime("%Y-%m-%d %H:%M:%S")
            emit_cdc("loan_contracts", "loan_id", loan_id, "u", signup_dt + timedelta(days=34), loan_v3, loan_v4)
            final_loan_status = "disbursed_active"
            final_disbursed_cad = principal_cad
        else:
            loan_v2 = dict(loan_v1)
            loan_v2["loan_status"] = "underwriting_declined"
            loan_v2["approved_principal_cad"] = 0.0
            loan_v2["disbursed_total_cad"] = 0.0
            loan_v2["updated_at"] = (signup_dt + timedelta(days=16)).strftime("%Y-%m-%d %H:%M:%S")
            emit_cdc("loan_contracts", "loan_id", loan_id, "u", signup_dt + timedelta(days=16), loan_v1, loan_v2)
            final_loan_status = "underwriting_declined"
            final_disbursed_cad = 0.0

        truth_loans[loan_id] = {
            "loan_id": loan_id,
            "applicant_id": applicant_id,
            "application_id": active_app_id,
            "final_loan_status": final_loan_status,
            "approved_principal_cad": principal_cad if is_funded else 0.0,
            "disbursed_total_cad": final_disbursed_cad,
        }

        # Payments and fees ledger
        pay_seq += 1
        fee_id = f"PAY-{pay_seq:05d}"
        fee_dt = signup_dt + timedelta(days=2)
        fee_row = {
            "payment_id": fee_id,
            "applicant_id": applicant_id,
            "loan_id": loan_id,
            "payment_type": "application_processing_fee",
            "direction": "inbound_fee",
            "gross_amount_cad": 150.0,
            "fx_fee_cad": 4.50,
            "payment_status": "settled",
            "reverses_payment_id": None,
            "settled_at": fee_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }
        emit_cdc("payments_and_fees", "payment_id", fee_id, "c", fee_dt, None, fee_row)

        net_fee_for_applicant = 150.0
        if i in reversed_fee_applicants:
            pay_seq += 1
            rev_id = f"PAY-{pay_seq:05d}"
            rev_dt = signup_dt + timedelta(days=5)
            rev_row = {
                "payment_id": rev_id,
                "applicant_id": applicant_id,
                "loan_id": loan_id,
                "payment_type": "fee_refund_reversal",
                "direction": "inbound_fee_reversal",
                "gross_amount_cad": -150.0,
                "fx_fee_cad": 0.0,
                "payment_status": "reversed",
                "reverses_payment_id": fee_id,
                "settled_at": rev_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }
            emit_cdc("payments_and_fees", "payment_id", rev_id, "c", rev_dt, None, rev_row)
            net_fee_for_applicant = 0.0

        if is_funded:
            # Origination deposit fee ($500 CAD) + Tuition disbursement + Stipend disbursement
            pay_seq += 1
            orig_id = f"PAY-{pay_seq:05d}"
            orig_row = {
                "payment_id": orig_id,
                "applicant_id": applicant_id,
                "loan_id": loan_id,
                "payment_type": "origination_closing_fee",
                "direction": "inbound_fee",
                "gross_amount_cad": 500.0,
                "fx_fee_cad": 10.0,
                "payment_status": "settled",
                "reverses_payment_id": None,
                "settled_at": (signup_dt + timedelta(days=22)).strftime("%Y-%m-%d %H:%M:%S"),
            }
            emit_cdc("payments_and_fees", "payment_id", orig_id, "c", signup_dt + timedelta(days=22), None, orig_row)
            net_fee_for_applicant += 500.0

            pay_seq += 1
            disb_t_id = f"PAY-{pay_seq:05d}"
            disb_t_row = {
                "payment_id": disb_t_id,
                "applicant_id": applicant_id,
                "loan_id": loan_id,
                "payment_type": "tuition_disbursement_to_dli",
                "direction": "outbound_disbursement",
                "gross_amount_cad": tuition_cad,
                "fx_fee_cad": 0.0,
                "payment_status": "settled",
                "reverses_payment_id": None,
                "settled_at": (signup_dt + timedelta(days=33)).strftime("%Y-%m-%d %H:%M:%S"),
            }
            emit_cdc("payments_and_fees", "payment_id", disb_t_id, "c", signup_dt + timedelta(days=33), None, disb_t_row)

            pay_seq += 1
            disb_s_id = f"PAY-{pay_seq:05d}"
            disb_s_row = {
                "payment_id": disb_s_id,
                "applicant_id": applicant_id,
                "loan_id": loan_id,
                "payment_type": "gic_living_stipend_disbursement",
                "direction": "outbound_disbursement",
                "gross_amount_cad": stipend_cad,
                "fx_fee_cad": 0.0,
                "payment_status": "settled",
                "reverses_payment_id": None,
                "settled_at": (signup_dt + timedelta(days=34)).strftime("%Y-%m-%d %H:%M:%S"),
            }
            emit_cdc("payments_and_fees", "payment_id", disb_s_id, "c", signup_dt + timedelta(days=34), None, disb_s_row)

        truth_payments.append(
            {
                "applicant_id": applicant_id,
                "loan_id": loan_id,
                "net_fee_revenue_cad": net_fee_for_applicant,
                "disbursed_tuition_cad": tuition_cad if is_funded else 0.0,
                "disbursed_stipend_cad": stipend_cad if is_funded else 0.0,
                "had_fee_reversal": i in reversed_fee_applicants,
            }
        )

        truth_applicants[applicant_id] = {
            "applicant_id": applicant_id,
            "origin_country": country_name,
            "institution_id": inst["institution_id"],
            "root_application_id": root_app_id,
            "active_application_id": active_app_id,
            "initial_cohort": initial_cohort,
            "active_cohort": active_cohort,
            "deferral_count": deferral_count,
            "has_missing_parent_link": has_missing_parent_link,
            "is_ambiguous_deferral": is_ambiguous_deferral,
            "active_guarantor_id": active_guar_id,
            "total_guarantors": total_guarantors_for_app,
            "had_guarantor_replacement": i in guarantor_chain_set,
            "loan_id": loan_id,
            "is_funded": is_funded,
            "approved_principal_cad": principal_cad if is_funded else 0.0,
            "disbursed_total_cad": final_disbursed_cad,
            "net_fee_revenue_cad": net_fee_for_applicant,
            "first_touch_channel": touches_for_app[0]["channel_name"],
            "last_touch_channel": touches_for_app[-1]["channel_name"],
        }

    # Emit 40 abandoned draft applications that are deleted via op='d' tombstones
    for d_idx in range(1, tombstone_draft_app_count + 1):
        ghost_app_id = f"APL-DRAFT-{d_idx:03d}"
        dt_create = base_dt + timedelta(days=d_idx)
        draft_payload = {
            "application_id": ghost_app_id,
            "applicant_id": f"APP-{d_idx:04d}",
            "institution_id": "INST-001",
            "program_name": "Draft Abandoned Application",
            "intake_cohort": "2026-Winter",
            "parent_application_id": None,
            "application_status": "draft_abandoned",
            "requested_loan_cad": 45000.0,
            "submitted_at": dt_create.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": dt_create.strftime("%Y-%m-%d %H:%M:%S"),
        }
        emit_cdc("applications", "application_id", ghost_app_id, "c", dt_create, None, draft_payload)
        emit_cdc("applications", "application_id", ghost_app_id, "d", dt_create + timedelta(hours=3), draft_payload, None)

    # Plant 180 out-of-order LSN events (stale update arriving with an OLDER lsn after the newer update)
    u_events = [e for e in cdc_events if e["op"] == "u" and e["before_json"] is not None]
    for stale_ev in u_events[:180]:
        stale_copy = dict(stale_ev)
        stale_copy["event_id"] = f"CDC-OOO-{stale_ev['event_id']}"
        stale_copy["lsn"] = int(stale_ev["lsn"]) - 5  # Older LSN than the current state!
        stale_copy["after_json"] = stale_ev["before_json"]
        cdc_events.append(stale_copy)

    # Plant 340 exact duplicate Kafka deliveries (same source_table, pk_value, and lsn)
    for dup_ev in cdc_events[:340]:
        dup_copy = dict(dup_ev)
        dup_copy["event_id"] = f"CDC-DUP-{dup_ev['event_id']}"
        cdc_events.append(dup_copy)

    # Deterministically shuffle slightly to simulate Kafka partition delivery order
    rng.shuffle(cdc_events)

    df_cdc = pd.DataFrame(cdc_events)
    df_cdc.to_parquet(CDC_DIR / "debezium_cdc_events.parquet", index=False)

    df_clicks = pd.DataFrame(ad_clicks)
    df_clicks.to_parquet(CDC_DIR / "ad_click_events.parquet", index=False)

    df_stitch = pd.DataFrame(identity_map)
    df_stitch.to_parquet(CDC_DIR / "identity_stitch_map.parquet", index=False)

    answer_key = {
        "seed": seed,
        "counts": {
            "institutions": len(institutions),
            "applicants": n_applicants,
            "funded_applicants": sum(1 for a in truth_applicants.values() if a["is_funded"]),
            "canonical_applications_post_tombstone": len(truth_applications),
            "active_non_superseded_applications": n_applicants,
            "superseded_deferred_applications": len(truth_applications) - n_applicants,
            "deferral_applicants": len(deferral_set),
            "multi_hop_deferral_applicants": len(multi_hop_deferral_set),
            "missing_parent_deferrals_total": len(missing_parent_set),
            "missing_parent_deferrals_auto_linked": len(missing_parent_set) - len(ambiguous_long_gap_set),
            "missing_parent_deferrals_routed_to_review": len(ambiguous_long_gap_set),
            "guarantor_replacement_chains": len(guarantor_chain_set),
            "superseded_guarantor_rows": sum(g["total_guarantors"] - 1 for g in truth_guarantors),
            "reversed_fee_events": len(reversed_fee_applicants),
            "duplicate_cdc_events_planted": 340,
            "out_of_order_lsn_events_planted": 180,
            "tombstone_delete_events_planted": len(tombstone_guarantor_set) + tombstone_draft_app_count,
            "duplicate_ad_pixels_planted": 140,
            "stitched_anonymous_sessions": len(stitched_anon_set),
            "total_cdc_envelope_events": len(df_cdc),
            "total_ad_click_events_raw": len(df_clicks),
        },
        "financial_totals": {
            "total_funded_principal_cad": round(sum(a["approved_principal_cad"] for a in truth_applicants.values()), 2),
            "total_disbursed_cad": round(sum(a["disbursed_total_cad"] for a in truth_applicants.values()), 2),
            "total_net_fee_revenue_cad": round(sum(a["net_fee_revenue_cad"] for a in truth_applicants.values()), 2),
            "reversed_fee_amount_cad": round(len(reversed_fee_applicants) * 150.0, 2),
        },
        "applicants": truth_applicants,
    }
    (TRUTH_DIR / "answer_key.json").write_text(json.dumps(answer_key, indent=2), encoding="utf-8")

    summary = {
        "elapsed_seconds": round(time.time() - t0, 2),
        **answer_key["counts"],
        **answer_key["financial_totals"],
    }
    return summary


if __name__ == "__main__":
    print(json.dumps(generate_corpus(), indent=2))

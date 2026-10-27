"""Warehouse claim checker and Debezium LSN receipt verifier."""
from __future__ import annotations

import re

PROHIBITED_WORDS = ("approximately", "probably", "roughly", "around", "estimated at", "maybe")
DOLLAR_RE = re.compile(r"\$[0-9][0-9,]*(?:\.\d{1,2})?")
ID_RE = re.compile(r"\b(?:APP-\d{4}|APL-\d{5}|GUA-\d{5}|LON-\d{4}|PAY-\d{5}|INST-\d{3})\b")


def check_summary_against_receipt(summary_text: str, allowed_numbers: set[int | float], allowed_ids: set[str]) -> dict:
    """Block any summary containing an uncited dollar figure, unknown entity ID, or speculative hedge word."""
    violations: list[str] = []
    lower = summary_text.lower()
    for word in PROHIBITED_WORDS:
        if re.search(rf"\b{re.escape(word)}\b", lower):
            violations.append(f"prohibited_word:{word}")

    allowed_rounded = {round(float(x), 2) for x in allowed_numbers} | {int(round(float(x))) for x in allowed_numbers}
    for m in DOLLAR_RE.findall(summary_text):
        val = float(m.replace("$", "").replace(",", ""))
        if round(val, 2) not in allowed_rounded and int(round(val)) not in allowed_rounded:
            violations.append(f"uncited_amount:{m}")

    for eid in ID_RE.findall(summary_text):
        if eid not in allowed_ids:
            violations.append(f"uncited_id:{eid}")

    return {
        "passed": len(violations) == 0,
        "violations": violations,
        "uncited_claims": len(violations),
    }


def hostile_summary_writer(applicant_id: str) -> str:
    """Test double that intentionally invents an uncited loan amount and uses a speculative word."""
    return (
        f"Applicant {applicant_id} was approved for approximately $99,450 in total loan principal "
        f"under guarantor GUA-99999."
    )

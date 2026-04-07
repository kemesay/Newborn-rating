"""
Cooperative Bank of Oromia — SME credit risk grading (spreadsheet-aligned).

Encodes scoring rules from *CoopBank Risk grading for SMEs.xlsx* where they can be
computed from this codebase’s data:
  - Credit History: Term/loan performance (CIC) + aggregate credit exposure (CIC)
  - Financial Position (40% block in “Financial 40” sheet) from Balance Sheet + Income Statement

Scores for CIC rows follow the Excel *Credit History 25%* sheet (decimal weights).
Financial sub-scores follow the *Financial 40* sheet totals per row (Manufacturing column by default).

Grade bands come from *Business Risk Summary* (achievement %).
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# CIC — Credit History (exact decimals from “Credit History 25” sheet)
# ---------------------------------------------------------------------------

COOPBANK_EXISTING_REPAYMENT_DECIMAL: Dict[str, float] = {
    "Regular repayment": 0.13,
    "1 - 30 days in arrears": 0.10,
    "31-90 days in arrears": 0.05,
    "More than 90 days in arrears": 0.0,
}

COOPBANK_SETTLED_LOANS_DECIMAL: Dict[str, float] = {
    "Settled with regular repayment": 0.08,
    "Settled timely but with an element of irregularity": 0.07,
    "Settled before sixty days after due date": 0.05,
    "Settled after 60 days but less than ninety days": 0.03,
    "Settled after being NPL and/or through foreclosure, legal action": 0.0,
}

# Aggregate Credit Exposure — “Credit Exposure (4%)” block
COOPBANK_EXPOSURE_MAX_DECIMAL = 0.04


def coopbank_aggregate_exposure_score(amount: float) -> Tuple[float, str, str]:
    """
    Map total **active** aggregate approved exposure to Excel score (max 0.04).

    Returns:
        (decimal_score, band_label, note)
    """
    if amount is None or amount <= 0:
        return 0.0, "No active approved exposure", "Sum of approved amounts on active CIC facilities is zero."

    if amount > 5_000_000:
        return 0.04, "> 5 million", ""
    if 3_000_000 <= amount <= 5_000_000:
        return 0.035, "3 - 5 million", ""
    if 2_000_000 <= amount < 3_000_000:
        return 0.025, "2 - 3 million", ""
    if 1_000_000 <= amount < 2_000_000:
        return 0.015, "1 - 2 million", ""
    return 0.01, "< 1 million", ""


def coopbank_bucket_existing_repayment(repayment_status: str) -> str:
    """Map internal repayment status string to CoopBank bucket label."""
    if not repayment_status:
        return "More than 90 days in arrears"
    if repayment_status == "Regular repayment":
        return "Regular repayment"
    if repayment_status == "1 - 30 days in arrears":
        return "1 - 30 days in arrears"
    if repayment_status in (
        "31-90 days in arrears",
        "31 - 60 days in arrears",
        "61 - 90 days in arrears",
    ):
        return "31-90 days in arrears"
    if repayment_status in (
        "91 - 180 days in arrears",
        "181 - 365 days in arrears",
        "365+ days in arrears",
        "Written off",
        "Status unknown",
        "More than 90 days in arrears",
    ):
        return "More than 90 days in arrears"
    return "More than 90 days in arrears"


def coopbank_bucket_settled_loan(settled_status: str, raw_text: str) -> str:
    """
    Map classifier output + raw CIC text to one of the five CoopBank settled buckets.
    """
    text = f"{settled_status} {raw_text}".lower()

    if any(
        k in text
        for k in [
            "npl",
            "foreclosure",
            "legal action",
            "lawsuit",
            "write-off",
            "written off",
            "charged off",
        ]
    ):
        return "Settled after being NPL and/or through foreclosure, legal action"

    if "timely" in text and "irregular" in text:
        return "Settled timely but with an element of irregularity"

    if re.search(r"before\s+(sixty|60)\s+days?\s+after\s+due", text):
        return "Settled before sixty days after due date"

    if re.search(r"after\s+(sixty|60)\s+days?", text) and re.search(
        r"(less\s+than|under)\s+(ninety|90)\s+days?", text
    ):
        return "Settled after 60 days but less than ninety days"

    if settled_status in (
        "Settled timely but with an element of irregularity",
    ):
        return "Settled timely but with an element of irregularity"

    if settled_status in ("Settled after lawsuit", "Settled after default", "Settled after write-off"):
        return "Settled after being NPL and/or through foreclosure, legal action"

    if settled_status == "Settled with delayed repayment":
        return "Settled after 60 days but less than ninety days"

    if "settled" in text or settled_status == "Settled with regular repayment":
        return "Settled with regular repayment"

    return "Settled with regular repayment"


def coopbank_term_loan_scores_for_accounts(
    enhanced_accounts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Per-account CoopBank T/Loan decimal scores and portfolio aggregates.

    Portfolio rule: report **mean** (balanced view) and **worst** (min decimal) across facilities.
    """
    per_account: List[Dict[str, Any]] = []
    decimals: List[float] = []

    for acc in enhanced_accounts:
        status = acc.get("account_status", "")
        raw = (
            f"{acc.get('credit_account_risk_classification', '')} "
            f"{acc.get('product_type', '')}"
        )
        if status == "Active":
            bucket = coopbank_bucket_existing_repayment(acc.get("repayment_status", ""))
            dec = COOPBANK_EXISTING_REPAYMENT_DECIMAL.get(bucket, 0.0)
        else:
            bucket = coopbank_bucket_settled_loan(
                acc.get("settled_loan_status", ""),
                raw,
            )
            dec = COOPBANK_SETTLED_LOANS_DECIMAL.get(bucket, 0.0)
        per_account.append(
            {
                "institution": acc.get("institution", ""),
                "account_number": acc.get("account_number", ""),
                "account_status": status,
                "coopbank_performance_bucket": bucket,
                "coopbank_term_loan_decimal": dec,
            }
        )
        decimals.append(dec)

    n = len(decimals)
    mean_dec = sum(decimals) / n if n else 0.0
    worst_dec = min(decimals) if decimals else 0.0

    return {
        "per_account": per_account,
        "term_loan_mean_decimal": mean_dec,
        "term_loan_worst_decimal": worst_dec,
        "term_loan_max_section_decimal": 0.21,
        "method": "mean_and_worst_facility",
    }


def coopbank_cic_credit_history_automation(
    enhanced_accounts: List[Dict[str, Any]],
    total_active_approved_exposure: float,
    aggregation: str = "mean",
) -> Dict[str, Any]:
    """
    CIC-automated slice of *Credit History* (T/Loan 21% + Aggregate exposure 4% in Excel notation).

    `aggregation`: "mean" | "worst" — which portfolio scalar to expose as primary term-loan score.
    """
    tl = coopbank_term_loan_scores_for_accounts(enhanced_accounts)
    term_primary = (
        tl["term_loan_worst_decimal"]
        if aggregation == "worst"
        else tl["term_loan_mean_decimal"]
    )
    exp_dec, exp_band, exp_note = coopbank_aggregate_exposure_score(total_active_approved_exposure)

    return {
        "framework": "CoopBank SME — Credit History (CIC-automated subset)",
        "term_loan_performance": {
            **tl,
            "primary_term_loan_decimal": term_primary,
            "aggregation_used": aggregation,
        },
        "aggregate_credit_exposure": {
            "total_active_approved_amount": total_active_approved_exposure,
            "decimal_score": exp_dec,
            "band": exp_band,
            "note": exp_note,
            "max_decimal": COOPBANK_EXPOSURE_MAX_DECIMAL,
        },
        "cic_automated_total_decimal": term_primary + exp_dec,
        "cic_automated_max_decimal": 0.21 + COOPBANK_EXPOSURE_MAX_DECIMAL,
    }


# ---------------------------------------------------------------------------
# Financial Position (40%) — Manufacturing column from “Financial 40” sheet
# ---------------------------------------------------------------------------

def _norm_label(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _find_item_amount(
    items: List[Dict[str, Any]],
    matchers: List[Callable[[str], bool]],
    amount_key: str = "amount_current",
) -> Optional[float]:
    for it in items:
        lab = _norm_label(str(it.get("label", "")))
        if any(m(lab) for m in matchers):
            v = it.get(amount_key)
            if isinstance(v, (int, float)):
                return float(v)
    return None


def extract_ratio_inputs(balance_sheet: Dict[str, Any], income_statement: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """Best-effort line matching for IFRS-like OCR labels."""
    bs_items: List[Dict[str, Any]] = balance_sheet.get("items") or []
    is_items: List[Dict[str, Any]] = income_statement.get("items") or []

    def has(*words: str) -> Callable[[str], bool]:
        return lambda lab: all(w in lab for w in words)

    total_assets = _find_item_amount(bs_items, [has("total", "assets")])
    total_liab = _find_item_amount(
        bs_items,
        [
            has("total", "liabilities"),
            lambda lab: "total liabilities" in lab and "equity" not in lab,
        ],
    )
    total_equity = _find_item_amount(
        bs_items,
        [
            has("total", "equity"),
            lambda lab: lab.startswith("total equity") and "liabilities" not in lab,
        ],
    )
    current_assets = _find_item_amount(bs_items, [has("current", "assets")])
    current_liab = _find_item_amount(bs_items, [has("current", "liabilities")])

    revenue = _find_item_amount(
        is_items,
        [
            has("revenue"),
            has("sales"),
            has("total", "revenue"),
            has("income", "from", "contracts"),
        ],
    )
    operating_profit = _find_item_amount(
        is_items,
        [
            has("operating", "profit"),
            has("profit", "from", "operations"),
            has("operating", "income"),
        ],
    )
    profit_before_tax = _find_item_amount(
        is_items,
        [
            has("profit", "before", "tax"),
            has("profit", "before", "income", "tax"),
        ],
    )
    net_income = _find_item_amount(
        is_items,
        [
            has("profit", "after", "tax"),
            has("profit", "for", "the", "year"),
            has("net", "income"),
            has("total", "comprehensive", "income"),
        ],
    )
    interest_exp = _find_item_amount(
        is_items,
        [
            has("finance", "cost"),
            has("interest", "expense"),
            has("interest", "payable"),
        ],
    )
    cogs = _find_item_amount(
        is_items,
        [
            has("cost", "of", "sales"),
            has("cost", "of", "goods"),
        ],
    )
    inventory = _find_item_amount(bs_items, [has("inventor")])

    # PBIT proxy for DSCR numerator (prefer PBT + interest; fallback operating profit)
    pbit: Optional[float] = None
    if profit_before_tax is not None and interest_exp is not None:
        pbit = profit_before_tax + abs(interest_exp)
    elif operating_profit is not None:
        pbit = operating_profit

    tangible_net_worth = total_equity  # TNW rarely labeled in extracts

    return {
        "total_assets": total_assets,
        "total_liabilities": total_liab,
        "total_equity": total_equity,
        "tangible_net_worth": tangible_net_worth,
        "current_assets": current_assets,
        "current_liabilities": current_liab,
        "revenue": revenue,
        "operating_profit": operating_profit,
        "profit_before_tax": profit_before_tax,
        "net_income": net_income,
        "interest_expense": interest_exp,
        "cost_of_sales": cogs,
        "inventory": inventory,
        "pbit": pbit,
    }


def _score_ratio(
    value: Optional[float],
    rules: List[Tuple[Callable[[float], bool], float, str]],
) -> Tuple[float, str]:
    if value is None:
        return 0.0, "missing input"
    for pred, pts, label in rules:
        try:
            if pred(value):
                return pts, label
        except Exception:
            continue
    return 0.0, "no match"


def compute_financial_position_40(
    balance_sheet: Dict[str, Any],
    income_statement: Dict[str, Any],
    *,
    sector: str = "manufacturing",
    financial_statement_quality: str = "all_audited_up_to_date",
    negative_values_adjustment: str = "none",
) -> Dict[str, Any]:
    """
    Sector column from *Financial 40* sheet (max contribution 0.40).

    `financial_statement_quality` keys:
      all_audited_up_to_date | audited_one_provisional | audited_two_provisional |
      all_provisional | commercial_credit_report

    `negative_values_adjustment`: none | minor | major
    """
    inp = extract_ratio_inputs(balance_sheet, income_statement)
    notes: List[str] = []

    sector_key = (sector or "manufacturing").strip().lower()
    supported_sectors = {
        "manufacturing",
        "agriculture",
        "import_dts",
        "export",
        "construction_machinery",
    }
    if sector_key not in supported_sectors:
        notes.append(f"Unsupported sector '{sector_key}', defaulted to manufacturing thresholds.")
        sector_key = "manufacturing"

    # --- Leverage: Total liabilities / Tangible net worth ---
    leverage: Optional[float] = None
    if inp["total_liabilities"] is not None and inp["tangible_net_worth"]:
        tnw = float(inp["tangible_net_worth"])
        if abs(tnw) > 1e-9:
            leverage = float(inp["total_liabilities"]) / tnw
        else:
            notes.append("Tangible net worth ~0; leverage not computed.")

    # From *Financial 40* sheet — leverage bands are the same across sectors.
    lev_score, lev_lbl = _score_ratio(
        leverage,
        [
            (lambda x: x <= 0.50, 0.06, "≤50%"),
            (lambda x: 0.50 < x <= 0.70, 0.04, "50%–70%"),
            (lambda x: 0.70 < x <= 0.90, 0.02, "70%–90%"),
            (lambda x: x > 0.90, 0.0, ">90%"),
        ],
    )

    # --- Liquidity CA/CL ---
    liq: Optional[float] = None
    if inp["current_assets"] is not None and inp["current_liabilities"]:
        cl = float(inp["current_liabilities"])
        if abs(cl) > 1e-9:
            liq = float(inp["current_assets"]) / cl
        else:
            notes.append("Current liabilities ~0; current ratio not computed.")

    if sector_key == "import_dts":
        liq_rules = [
            (lambda x: x > 1.5, 0.05, ">1.5"),
            (lambda x: 1.0 < x <= 1.5, 0.04, "1.0–1.5"),
            (lambda x: 0.75 < x <= 1.0, 0.03, "0.75–1.0"),
            (lambda x: 0.5 < x <= 0.75, 0.02, "0.5–0.75"),
            (lambda x: x <= 0.5, 0.0, "<0.5"),
        ]
    elif sector_key == "export":
        liq_rules = [
            (lambda x: x > 1.25, 0.05, ">1.25"),
            (lambda x: 1.0 < x <= 1.25, 0.04, "1.0–1.25"),
            (lambda x: 0.75 < x <= 1.0, 0.03, "0.75–1.0"),
            (lambda x: 0.5 < x <= 0.75, 0.02, "0.5–0.75"),
            (lambda x: x <= 0.5, 0.0, "<0.5"),
        ]
    elif sector_key == "construction_machinery":
        liq_rules = [
            (lambda x: x > 2.0, 0.05, ">2"),
            (lambda x: 1.5 < x <= 2.0, 0.04, "1.5–2.0"),
            (lambda x: 1.0 < x <= 1.5, 0.03, "1–1.5"),
            (lambda x: 0.5 < x <= 1.0, 0.02, "0.5–1"),
            (lambda x: x <= 0.5, 0.0, "<0.5"),
        ]
    elif sector_key == "agriculture":
        liq_rules = [
            (lambda x: x > 2.0, 0.05, ">2"),
            (lambda x: 1.5 < x <= 2.0, 0.04, "1.5–2.0"),
            (lambda x: 1.25 < x <= 1.5, 0.03, "1.25–1.5"),
            (lambda x: 0.75 < x <= 1.25, 0.02, "0.75–1.25"),
            (lambda x: x <= 0.75, 0.0, "<0.75"),
        ]
    else:
        liq_rules = [
            (lambda x: x > 2.0, 0.05, ">2"),
            (lambda x: 1.5 < x <= 2.0, 0.04, "1.5–2.0"),
            (lambda x: 1.0 < x <= 1.5, 0.03, "1–1.5"),
            (lambda x: 0.5 < x <= 1.0, 0.02, "0.5–1"),
            (lambda x: x <= 0.5, 0.0, "<0.5"),
        ]
    liq_score, liq_lbl = _score_ratio(
        liq,
        liq_rules,
    )

    # --- ROA = NI / Total assets ---
    roa: Optional[float] = None
    if inp["net_income"] is not None and inp["total_assets"]:
        ta = float(inp["total_assets"])
        if abs(ta) > 1e-9:
            roa = float(inp["net_income"]) / ta

    if sector_key == "import_dts":
        roa_rules = [
            (lambda x: x >= 0.15, 0.03, "≥15%"),
            (lambda x: 0.10 <= x < 0.15, 0.025, "10%–15%"),
            (lambda x: 0.08 <= x < 0.10, 0.02, "8%–10%"),
            (lambda x: 0.04 <= x < 0.08, 0.01, "4%–8%"),
            (lambda x: 0.01 <= x < 0.04, 0.005, "1%–4%"),
            (lambda x: x < 0.01, 0.0, "<1%"),
        ]
    elif sector_key == "export":
        roa_rules = [
            (lambda x: x >= 0.15, 0.03, "≥15%"),
            (lambda x: 0.10 <= x < 0.15, 0.025, "10%–15%"),
            (lambda x: 0.08 <= x < 0.10, 0.02, "8%–10%"),
            (lambda x: 0.04 <= x < 0.08, 0.01, "4%–8%"),
            (lambda x: 0.01 <= x < 0.04, 0.005, "1%–4%"),
            (lambda x: x < 0.01, 0.0, "<1%"),
        ]
    elif sector_key == "construction_machinery":
        roa_rules = [
            (lambda x: x >= 0.15, 0.03, "≥15%"),
            (lambda x: 0.10 <= x < 0.15, 0.025, "10%–15%"),
            (lambda x: 0.08 <= x < 0.10, 0.02, "8%–10%"),
            (lambda x: 0.04 <= x < 0.08, 0.01, "4%–8%"),
            (lambda x: 0.01 <= x < 0.04, 0.005, "1%–4%"),
            (lambda x: x < 0.01, 0.0, "<1%"),
        ]
    elif sector_key == "agriculture":
        roa_rules = [
            (lambda x: x >= 0.15, 0.03, "≥15%"),
            (lambda x: 0.10 <= x < 0.15, 0.025, "10%–15%"),
            (lambda x: 0.07 <= x < 0.10, 0.02, "7%–10%"),
            (lambda x: 0.03 <= x < 0.07, 0.01, "3%–7%"),
            (lambda x: 0.01 <= x < 0.03, 0.005, "1%–3%"),
            (lambda x: x < 0.01, 0.0, "<1%"),
        ]
    else:
        roa_rules = [
            (lambda x: x >= 0.10, 0.03, "≥10%"),
            (lambda x: 0.08 <= x < 0.10, 0.025, "8%–10%"),
            (lambda x: 0.06 <= x < 0.08, 0.02, "6%–8%"),
            (lambda x: 0.03 <= x < 0.06, 0.01, "3%–6%"),
            (lambda x: 0.01 <= x < 0.03, 0.005, "1%–3%"),
            (lambda x: x < 0.01, 0.0, "<1%"),
        ]
    roa_score, roa_lbl = _score_ratio(
        roa,
        roa_rules,
    )

    # --- Operating margin = Operating profit / Revenue ---
    margin: Optional[float] = None
    if inp["operating_profit"] is not None and inp["revenue"]:
        rev = float(inp["revenue"])
        if abs(rev) > 1e-9:
            margin = float(inp["operating_profit"]) / rev

    if sector_key == "import_dts":
        om_rules = [
            (lambda x: x >= 0.20, 0.05, "≥20%"),
            (lambda x: 0.15 <= x < 0.20, 0.04, "15%–20%"),
            (lambda x: 0.10 <= x < 0.15, 0.03, "10%–15%"),
            (lambda x: 0.05 <= x < 0.10, 0.02, "5%–10%"),
            (lambda x: x < 0.05, 0.01, "<5%"),
        ]
    elif sector_key == "export":
        om_rules = [
            (lambda x: x >= 0.23, 0.05, "≥23%"),
            (lambda x: 0.16 <= x < 0.23, 0.04, "16%–22%"),
            (lambda x: 0.11 <= x < 0.16, 0.03, "11%–16%"),
            (lambda x: 0.06 <= x < 0.11, 0.02, "6%–11%"),
            (lambda x: x < 0.06, 0.01, "<6%"),
        ]
    elif sector_key == "construction_machinery":
        om_rules = [
            (lambda x: x >= 0.20, 0.05, "≥20%"),
            (lambda x: 0.15 <= x < 0.20, 0.04, "15%–20%"),
            (lambda x: 0.10 <= x < 0.15, 0.03, "10%–15%"),
            (lambda x: 0.05 <= x < 0.10, 0.02, "5%–10%"),
            (lambda x: x < 0.05, 0.01, "<5%"),
        ]
    elif sector_key == "agriculture":
        om_rules = [
            (lambda x: x >= 0.22, 0.05, "≥22%"),
            (lambda x: 0.16 <= x < 0.22, 0.04, "16%–22%"),
            (lambda x: 0.11 <= x < 0.16, 0.03, "11%–16%"),
            (lambda x: 0.06 <= x < 0.11, 0.02, "6%–11%"),
            (lambda x: x < 0.06, 0.01, "<6%"),
        ]
    else:
        om_rules = [
            (lambda x: x >= 0.20, 0.05, "≥20%"),
            (lambda x: 0.15 <= x < 0.20, 0.04, "15%–20%"),
            (lambda x: 0.10 <= x < 0.15, 0.03, "10%–15%"),
            (lambda x: 0.05 <= x < 0.10, 0.02, "5%–10%"),
            (lambda x: x < 0.05, 0.01, "<5%"),
        ]
    om_score, om_lbl = _score_ratio(
        margin,
        om_rules,
    )

    # --- DSCR = Net income after tax / Interest (per sheet) ---
    dscr: Optional[float] = None
    if inp["net_income"] is not None and inp["interest_expense"] is not None:
        ie = float(inp["interest_expense"])
        if abs(ie) > 1e-9:
            dscr = float(inp["net_income"]) / abs(ie)

    if sector_key == "import_dts":
        dscr_rules = [
            (lambda x: x > 2.5, 0.03, ">2.5"),
            (lambda x: 2.0 < x <= 2.5, 0.025, "2–2.5"),
            (lambda x: 1.5 < x <= 2.0, 0.02, "1.5–2"),
            (lambda x: 1.0 < x <= 1.5, 0.01, "1–1.5"),
            (lambda x: x <= 1.0, 0.0, "<1"),
        ]
    elif sector_key == "export":
        dscr_rules = [
            (lambda x: x > 3.0, 0.03, ">3"),
            (lambda x: 2.0 < x <= 3.0, 0.025, "2–3"),
            (lambda x: 1.0 < x <= 2.0, 0.02, "1–2"),
            (lambda x: 0.5 < x <= 1.0, 0.01, "0.5–1"),
            (lambda x: x <= 0.5, 0.0, "<0.5"),
        ]
    elif sector_key == "construction_machinery":
        dscr_rules = [
            (lambda x: x > 2.0, 0.03, ">2"),
            (lambda x: 1.5 < x <= 2.0, 0.025, "1.5–2"),
            (lambda x: 1.0 < x <= 1.5, 0.02, "1–1.5"),
            (lambda x: 0.5 < x <= 1.0, 0.01, "0.5–1"),
            (lambda x: x <= 0.5, 0.0, "<0.5"),
        ]
    elif sector_key == "agriculture":
        dscr_rules = [
            (lambda x: x > 2.5, 0.03, ">2.5"),
            (lambda x: 1.5 < x <= 2.5, 0.025, "1.5–2.5"),
            (lambda x: 1.0 < x <= 1.5, 0.02, "1–1.5"),
            (lambda x: 0.5 < x <= 1.0, 0.01, "0.5–1"),
            (lambda x: x <= 0.5, 0.0, "<0.5"),
        ]
    else:
        dscr_rules = [
            (lambda x: x > 2.5, 0.03, ">2.5"),
            (lambda x: 2.0 < x <= 2.5, 0.025, "2–2.5"),
            (lambda x: 1.5 < x <= 2.0, 0.02, "1.5–2"),
            (lambda x: 1.0 < x <= 1.5, 0.01, "1–1.5"),
            (lambda x: x <= 1.0, 0.0, "<1"),
        ]
    dscr_score, dscr_lbl = _score_ratio(
        dscr,
        dscr_rules,
    )

    # --- Inventory turnover = COGS / Inventory ---
    inv_turn: Optional[float] = None
    if inp["cost_of_sales"] is not None and inp["inventory"]:
        inv = float(inp["inventory"])
        if abs(inv) > 1e-9:
            inv_turn = float(inp["cost_of_sales"]) / inv

    if sector_key == "import_dts":
        inv_rules = [
            (lambda x: x > 8, 0.04, ">8"),
            (lambda x: 4 <= x <= 8, 0.03, "4–8"),
            (lambda x: 2 <= x < 4, 0.02, "2–4"),
            (lambda x: x < 2, 0.01, "<2"),
        ]
    elif sector_key == "export":
        inv_rules = [
            (lambda x: x > 4, 0.04, ">4"),
            (lambda x: 2 <= x <= 4, 0.03, "2–4"),
            (lambda x: 1 <= x < 2, 0.02, "1–2"),
            (lambda x: x < 1, 0.01, "<1"),
        ]
    elif sector_key == "construction_machinery":
        inv_rules = [
            (lambda x: x > 10, 0.04, ">10"),
            (lambda x: 7 <= x <= 10, 0.03, "7–10"),
            (lambda x: 3 <= x < 7, 0.02, "3–7"),
            (lambda x: x < 3, 0.01, "<3"),
        ]
    elif sector_key == "agriculture":
        inv_rules = [
            (lambda x: x > 3, 0.04, ">3"),
            (lambda x: 2 <= x <= 3, 0.03, "2–3"),
            (lambda x: 1 <= x < 2, 0.02, "1–2"),
            (lambda x: x < 1, 0.01, "<1"),
        ]
    else:
        inv_rules = [
            (lambda x: x > 12, 0.04, ">12"),
            (lambda x: 8 < x <= 12, 0.03, "8–12"),
            (lambda x: 4 < x <= 8, 0.02, "4–8"),
            (lambda x: x <= 4, 0.01, "<4"),
        ]
    inv_score, inv_lbl = _score_ratio(
        inv_turn,
        inv_rules,
    )

    # --- ROE = NI / Equity ---
    roe: Optional[float] = None
    if inp["net_income"] is not None and inp["total_equity"]:
        eq = float(inp["total_equity"])
        if abs(eq) > 1e-9:
            roe = float(inp["net_income"]) / eq
    roe_score, roe_lbl = _score_ratio(
        roe,
        [
            (lambda x: x > 0.15, 0.04, ">15%"),
            (lambda x: 0.12 <= x <= 0.15, 0.03, "12%–15%"),
            (lambda x: 0.10 <= x < 0.12, 0.02, "10%–12%"),
            (lambda x: 0.05 <= x < 0.10, 0.01, "5%–10%"),
            (lambda x: x < 0.05, 0.0, "<5%"),
        ],
    )

    fs_quality_scores = {
        "all_audited_up_to_date": 0.05,
        "audited_one_provisional": 0.04,
        "audited_two_provisional": 0.03,
        "all_provisional": 0.02,
        "commercial_credit_report": 0.01,
    }
    qual_score = fs_quality_scores.get(financial_statement_quality, 0.05)

    neg_adj = {"none": 0.0, "minor": -0.02, "major": -0.05}.get(
        negative_values_adjustment, 0.0
    )

    eq_ratio: Optional[float] = None
    if inp["total_equity"] is not None and inp["total_assets"]:
        ta = float(inp["total_assets"])
        if abs(ta) > 1e-9:
            eq_ratio = float(inp["total_equity"]) / ta
    eq_score, eq_lbl = _score_ratio(
        eq_ratio,
        [
            (lambda x: x >= 0.50, 0.05, "≥50%"),
            (lambda x: 0.40 <= x < 0.50, 0.04, "40%–50%"),
            (lambda x: 0.35 <= x < 0.40, 0.03, "35%–40%"),
            (lambda x: 0.30 <= x < 0.35, 0.02, "30%–35%"),
            (lambda x: x < 0.30, 0.0, "<30%"),
        ],
    )

    components = {
        "leverage_tl_tnw": {"value": leverage, "band": lev_lbl, "decimal": lev_score},
        "liquidity_ca_cl": {"value": liq, "band": liq_lbl, "decimal": liq_score},
        "roa": {"value": roa, "band": roa_lbl, "decimal": roa_score},
        "operating_margin": {"value": margin, "band": om_lbl, "decimal": om_score},
        "dscr_net_income_interest": {"value": dscr, "band": dscr_lbl, "decimal": dscr_score},
        "inventory_turnover_cogs_inv": {"value": inv_turn, "band": inv_lbl, "decimal": inv_score},
        "roe": {"value": roe, "band": roe_lbl, "decimal": roe_score},
        "quality_of_financial_statements": {
            "value": financial_statement_quality,
            "decimal": qual_score,
        },
        "equity_to_total_assets": {"value": eq_ratio, "band": eq_lbl, "decimal": eq_score},
        "negative_values_adjustment": {"decimal": neg_adj},
    }

    total = (
        lev_score
        + liq_score
        + roa_score
        + om_score
        + dscr_score
        + inv_score
        + roe_score
        + qual_score
        + eq_score
        + neg_adj
    )
    headline = max(0.0, min(0.40, total))

    return {
        "sector_profile": f"{sector_key.title()} (Financial 40 sheet)",
        "inputs": inp,
        "components": components,
        "financial_position_decimal": round(headline, 6),
        "financial_position_max_decimal": 0.40,
        "achievement_pct_of_block": round((headline / 0.40) * 100.0, 2),
        "notes": notes,
    }


def management_capacity_9(
    *,
    experience_years: float,
    qualification: str,
    duty_segregation: str,
) -> Dict[str, Any]:
    """
    *Management Capacity 9%* sheet (max 0.09).
    """
    y = float(experience_years or 0.0)
    if y > 10:
        exp = 0.04
        exp_band = ">10 years"
    elif 7 <= y <= 10:
        exp = 0.03
        exp_band = "7–10 years"
    elif 3 <= y < 7:
        exp = 0.02
        exp_band = "3–7 years"
    else:
        exp = 0.01
        exp_band = "<3 years"

    qmap = {
        "degree_and_above": 0.03,
        "diploma_and_above": 0.02,
        "high_school_and_above": 0.01,
        "below_high_school": 0.0,
    }
    q_score = qmap.get((qualification or "").strip().lower(), 0.01)

    dmap = {
        "clear": 0.02,
        "semi": 0.01,
        "unclear": 0.0,
    }
    d_score = dmap.get((duty_segregation or "").strip().lower(), 0.01)

    total = exp + q_score + d_score
    return {
        "components": {
            "experience": {"decimal": exp, "band": exp_band},
            "qualification": {"decimal": q_score, "choice": qualification},
            "duty_segregation": {"decimal": d_score, "choice": duty_segregation},
        },
        "management_decimal": round(min(0.09, total), 4),
        "management_max_decimal": 0.09,
    }


def integrity_6(
    *,
    consistency: str,
    responsiveness: str,
    tax_payment: str,
) -> Dict[str, Any]:
    """
    *Integrity 6%* sheet (max 0.06).
    """
    cmap = {"adequate": 0.02, "acceptable": 0.01, "inconsistent": 0.0}
    rmap = {
        "provides_consistently_prompt": 0.02,
        "response_and_willingly": 0.015,
        "responds_with_repeated_inquiry": 0.01,
        "not_responsive_or_willing": 0.0,
    }
    tmap = {"perfect": 0.02, "acceptable": 0.01, "inconsistent": 0.0}

    c = cmap.get((consistency or "").strip().lower(), 0.01)
    r = rmap.get((responsiveness or "").strip().lower(), 0.01)
    t = tmap.get((tax_payment or "").strip().lower(), 0.01)
    total = c + r + t
    return {
        "components": {
            "consistency_of_documents": {"decimal": c, "choice": consistency},
            "responsiveness": {"decimal": r, "choice": responsiveness},
            "tax_payment": {"decimal": t, "choice": tax_payment},
        },
        "integrity_decimal": round(min(0.06, total), 4),
        "integrity_max_decimal": 0.06,
    }


def banking_relationship_10(
    *,
    relationship_years: float,
    transaction_share_pct: float,
) -> Dict[str, Any]:
    """
    *Banking Relationship* sheet (max 0.10).
    """
    y = float(relationship_years or 0.0)
    if y > 3:
        rel = 0.04
        rel_band = ">3 years"
    elif 2 <= y <= 3:
        rel = 0.02
        rel_band = "2–3 years"
    elif 1 <= y < 2:
        rel = 0.01
        rel_band = "1–2 years"
    else:
        rel = 0.005
        rel_band = "<1 year"

    p = float(transaction_share_pct or 0.0)
    if p > 100:
        share = 0.06
        share_band = ">100%"
    elif 90 <= p < 100:
        share = 0.05
        share_band = "90–99.9%"
    elif 80 <= p < 90:
        share = 0.04
        share_band = "80–89.9%"
    elif 70 <= p < 80:
        share = 0.03
        share_band = "70–79.9%"
    elif 60 <= p < 70:
        share = 0.02
        share_band = "60–69.9%"
    elif 50 <= p < 60:
        share = 0.01
        share_band = "50–59.9%"
    else:
        share = 0.0
        share_band = "<50%"

    total = rel + share
    return {
        "components": {
            "relationship_length": {"decimal": rel, "band": rel_band},
            "transaction_share": {"decimal": share, "band": share_band},
        },
        "banking_decimal": round(min(0.10, total), 4),
        "banking_max_decimal": 0.10,
    }


def interpret_business_risk_grade(achievement_pct: float) -> Dict[str, str]:
    """
    *Business Risk Summary* — achievement percentage → Roman grade, risk level, bankability.
    """
    if achievement_pct >= 95:
        return {
            "grade": "I",
            "risk_level": "Exceptionally low Risk",
            "bankability": "Prime (Prominent) (bankable)",
        }
    if achievement_pct >= 80:
        return {
            "grade": "II",
            "risk_level": "Very Low Risk",
            "bankability": "Reputable (Bankable)",
        }
    if achievement_pct >= 65:
        return {
            "grade": "III",
            "risk_level": "Low Risk",
            "bankability": "Acceptable (Bankable)",
        }
    if achievement_pct >= 50:
        return {
            "grade": "IV",
            "risk_level": "Moderate risk",
            "bankability": "Watch list (Bankable)",
        }
    return {
        "grade": "V",
        "risk_level": "High risk",
        "bankability": "Doubtful (Un-bankable)",
    }


def industry_attractiveness_10(
    business_outlook: str,
    market_competition: str,
    form_of_organization: str,
) -> Dict[str, Any]:
    """
    *Industry attractiveness 10%* sheet — categorical scoring (max 0.10).
    """
    outlook = {
        "favorable": 0.04,
        "stable": 0.02,
        "unstable": 0.0,
    }
    comp = {
        "dominant player": 0.03,
        "acceptable": 0.02,
        "weak player": 0.0,
    }
    org = {
        "cooperatives": 0.03,
        "share company": 0.02,
        "private limited company": 0.02,
        "sole proprietorship": 0.01,
    }
    o = outlook.get(business_outlook.lower().strip(), 0.02)
    c = comp.get(market_competition.lower().strip(), 0.02)
    f = org.get(form_of_organization.lower().strip(), 0.02)
    total = min(0.10, o + c + f)
    return {
        "components": {
            "business_outlook": o,
            "market_competition": c,
            "form_of_organization": f,
        },
        "industry_decimal": round(total, 4),
        "industry_max_decimal": 0.10,
    }

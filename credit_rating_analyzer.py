"""
Credit Rating Analysis Module
Expert-level credit risk assessment and classification system
"""
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field
import logging

from coopbank_sme_grading import (
    coopbank_cic_credit_history_automation,
    coopbank_term_loan_scores_for_accounts,
)

logger = logging.getLogger(__name__)

@dataclass
class CreditExposure:
    """Aggregated credit exposure metrics"""
    total_approved_amount: float = 0.0
    total_current_balance: float = 0.0
    total_outstanding: float = 0.0
    total_by_risk_classification: Dict[str, float] = field(default_factory=dict)
    total_by_product_type: Dict[str, float] = field(default_factory=dict)
    total_by_institution: Dict[str, float] = field(default_factory=dict)
    active_accounts_count: int = 0
    settled_accounts_count: int = 0

class RepaymentStatus:
    """Repayment status categories for existing loans"""
    REGULAR = "Regular repayment"
    DAYS_1_30_ARREARS = "1 - 30 days in arrears"
    DAYS_31_90_ARREARS = "31-90 days in arrears"
    DAYS_31_60_ARREARS = "31 - 60 days in arrears"
    DAYS_61_90_ARREARS = "61 - 90 days in arrears"
    DAYS_91_180_ARREARS = "91 - 180 days in arrears"
    DAYS_181_365_ARREARS = "181 - 365 days in arrears"
    DAYS_365_PLUS_ARREARS = "365+ days in arrears"
    WRITTEN_OFF = "Written off"
    UNKNOWN = "Status unknown"
    
    STATUS_PATTERNS = {
        REGULAR: [
            r"regular\s+repayment",
            r"current",
            r"up\s+to\s+date",
            r"on\s+time",
            r"performing",
            r"normal"
        ],
        DAYS_1_30_ARREARS: [
            r"1\s*-\s*30\s+days?",
            r"1\s+to\s+30\s+days?",
            r"0\s*-\s*30\s+days?",
            r"days?\s+overdue",
            r"past\s+due\s+1"
        ],
        DAYS_31_90_ARREARS: [
            r"31\s*-\s*90\s+days?",
            r"31\s+to\s+90\s+days?",
            r"30\s*-\s*90\s+days?",
        ],
        DAYS_31_60_ARREARS: [
            r"31\s*-\s*60\s+days?",
            r"31\s+to\s+60\s+days?",
            r"past\s+due\s+2"
        ],
        DAYS_61_90_ARREARS: [
            r"61\s*-\s*90\s+days?",
            r"61\s+to\s+90\s+days?",
            r"past\s+due\s+3"
        ],
        DAYS_91_180_ARREARS: [
            r"91\s*-\s*180\s+days?",
            r"91\s+to\s+180\s+days?",
            r"3\s+to\s+6\s+months?"
        ],
        DAYS_181_365_ARREARS: [
            r"181\s*-\s*365\s+days?",
            r"181\s+to\s+365\s+days?",
            r"6\s+to\s+12\s+months?",
            r"over\s+6\s+months?"
        ],
        DAYS_365_PLUS_ARREARS: [
            r"365\+\s+days?",
            r"over\s+365\s+days?",
            r"over\s+1\s+year",
            r"12\+\s+months?",
            r"non\s*performing"
        ],
        WRITTEN_OFF: [
            r"written\s+off",
            r"written\s*off",
            r"wo",
            r"charged\s+off"
        ]
    }

class SettledLoanStatus:
    """Status categories for settled loans"""
    SETTLED_REGULAR = "Settled with regular repayment"
    SETTLED_TIMELY_IRREGULAR = "Settled timely but with an element of irregularity"
    SETTLED_DELAYED = "Settled with delayed repayment"
    SETTLED_RESTRUCTURED = "Settled after restructuring"
    SETTLED_WRITTEN_OFF = "Settled after write-off"
    SETTLED_LAWSUIT = "Settled after lawsuit"
    SETTLED_DEFAULT = "Settled after default"
    UNKNOWN = "Settlement status unknown"
    
    STATUS_PATTERNS = {
        SETTLED_REGULAR: [
            r"settled\s+(with\s+)?regular",
            r"settled\s+normally",
            r"settled\s+on\s+time",
            r"regular\s+settlement"
        ],
        SETTLED_TIMELY_IRREGULAR: [
            r"settled\s+timely\s+but\s+.*irregular",
            r"settled\s+with\s+.*irregularity",
            r"timely\s+settlement\s+.*irregular"
        ],
        SETTLED_DELAYED: [
            r"settled\s+with\s+delayed",
            r"delayed\s+settlement",
            r"settled\s+late"
        ],
        SETTLED_RESTRUCTURED: [
            r"settled\s+after\s+restructuring",
            r"restructured\s+.*settled",
            r"settled\s+.*restructured"
        ],
        SETTLED_WRITTEN_OFF: [
            r"settled\s+after\s+write[\s\-]?off",
            r"write[\s\-]?off\s+.*settled",
            r"wo\s+.*settled"
        ],
        SETTLED_LAWSUIT: [
            r"settled\s+after\s+lawsuit",
            r"settled\s+.*legal",
            r"lawsuit\s+.*settled"
        ],
        SETTLED_DEFAULT: [
            r"settled\s+after\s+default",
            r"default\s+.*settled",
            r"settled\s+.*defaulted"
        ]
    }

def parse_amount(amount_str: str) -> float:
    """Parse amount string to float, handling various formats"""
    if not amount_str or amount_str.strip() == "":
        return 0.0
    
    # Remove currency symbols, commas, and whitespace
    cleaned = re.sub(r'[,\s]', '', str(amount_str))
    cleaned = re.sub(r'[^\d.]', '', cleaned)
    
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0

def classify_repayment_status(risk_classification: str, payment_due_date: str = "") -> str:
    """
    Classify repayment status based on risk classification and payment due date.
    
    Args:
        risk_classification: Credit account risk classification string
        payment_due_date: Payment due date (optional, for future use)
    
    Returns:
        Repayment status category
    """
    if not risk_classification:
        return RepaymentStatus.UNKNOWN
    
    text = risk_classification.lower().strip()
    
    # Check each status pattern
    for status, patterns in RepaymentStatus.STATUS_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return status
    
    # Check for common risk classification codes
    if re.search(r'^pass|^normal|^1$', text):
        return RepaymentStatus.REGULAR
    elif re.search(r'^special\s+mention|^2$', text):
        return RepaymentStatus.DAYS_1_30_ARREARS
    elif re.search(r'^substandard|^3$', text):
        return RepaymentStatus.DAYS_31_60_ARREARS
    elif re.search(r'^doubtful|^4$', text):
        return RepaymentStatus.DAYS_61_90_ARREARS
    elif re.search(r'^loss|^5$', text):
        return RepaymentStatus.DAYS_365_PLUS_ARREARS
    
    return RepaymentStatus.UNKNOWN

def classify_settled_loan_status(text: str) -> str:
    """
    Classify settled loan status based on text description.
    
    Args:
        text: Text describing the settlement status
    
    Returns:
        Settled loan status category
    """
    if not text:
        return SettledLoanStatus.UNKNOWN
    
    text_lower = text.lower().strip()
    
    # Check each status pattern
    for status, patterns in SettledLoanStatus.STATUS_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return status
    
    # Check if it mentions "settled" at all
    if "settled" in text_lower:
        return SettledLoanStatus.SETTLED_REGULAR  # Default to regular if just "settled"
    
    return SettledLoanStatus.UNKNOWN

def is_settled_account(account: Dict[str, Any]) -> bool:
    """Check if an account is settled based on various indicators"""
    risk_class = account.get("credit_account_risk_classification", "").lower()
    product_type = account.get("product_type", "").lower()
    current_balance = account.get("current_balance_amount", "")
    
    # Check if explicitly marked as settled
    if "settled" in risk_class or "settled" in product_type:
        return True
    
    # Check if balance is zero or very small (common for settled accounts)
    balance = parse_amount(current_balance)
    if balance == 0.0:
        return True
    
    # Check for settlement keywords
    settlement_keywords = ["closed", "settled", "paid off", "fully paid"]
    for keyword in settlement_keywords:
        if keyword in risk_class or keyword in product_type:
            return True
    
    return False

def calculate_credit_exposure(credit_accounts: List[Dict[str, Any]]) -> CreditExposure:
    """
    Calculate aggregate credit exposure from credit accounts.
    
    Args:
        credit_accounts: List of credit account dictionaries
    
    Returns:
        CreditExposure object with aggregated metrics
    """
    exposure = CreditExposure()
    
    for account in credit_accounts:
        # Parse amounts
        approved = parse_amount(account.get("approved_amount", ""))
        current_balance = parse_amount(account.get("current_balance_amount", ""))
        
        # Determine if settled
        is_settled = is_settled_account(account)
        
        if is_settled:
            exposure.settled_accounts_count += 1
        else:
            exposure.active_accounts_count += 1
            exposure.total_approved_amount += approved
            exposure.total_current_balance += current_balance
            exposure.total_outstanding += current_balance
        
        # Aggregate by risk classification
        risk_class = account.get("credit_account_risk_classification", "Unknown")
        if risk_class:
            if risk_class not in exposure.total_by_risk_classification:
                exposure.total_by_risk_classification[risk_class] = 0.0
            exposure.total_by_risk_classification[risk_class] += current_balance
        
        # Aggregate by product type
        product_type = account.get("product_type", "Unknown")
        if product_type:
            if product_type not in exposure.total_by_product_type:
                exposure.total_by_product_type[product_type] = 0.0
            exposure.total_by_product_type[product_type] += current_balance
        
        # Aggregate by institution
        institution = account.get("institution", "Unknown")
        if institution:
            if institution not in exposure.total_by_institution:
                exposure.total_by_institution[institution] = 0.0
            exposure.total_by_institution[institution] += current_balance
    
    return exposure


def classify_facility_type(account: Dict[str, Any]) -> str:
    """
    Classify facility type into:
    - Revolving Facility
    - Non-Revolving loan
    - Other/Unknown

    Uses best-effort keyword matching on `product_type` and `credit_type`.
    """
    text = f"{account.get('product_type', '')} {account.get('credit_type', '')}".lower()

    revolving_keywords = [
        "revolving",
        "overdraft",
        "od",
        "credit line",
        "line of credit",
        "limit",
        "rc",
    ]
    non_revolving_keywords = [
        "non-revolving",
        "non revolving",
        "term loan",
        "term",
        "installment",
        "mortgage",
        "lease",
        "project",
        "machinery loan",
        "merchandise loan",
    ]

    if any(k in text for k in revolving_keywords):
        return "Revolving Facility"
    if any(k in text for k in non_revolving_keywords):
        return "Non-Revolving loan"
    return "Other/Unknown"

def analyze_credit_accounts(credit_accounts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Comprehensive credit rating analysis of credit accounts.
    
    Args:
        credit_accounts: List of credit account dictionaries
    
    Returns:
        Dictionary with comprehensive analysis including:
        - repayment_status_breakdown
        - settled_loans_breakdown
        - credit_exposure
        - enhanced_accounts (accounts with added classifications)
    """
    logger.info(f"🔍 Starting comprehensive credit rating analysis for {len(credit_accounts)} accounts")
    
    # Classify accounts and enhance with repayment/settlement status
    enhanced_accounts = []
    repayment_status_count = {}
    settled_loans_count = {}
    
    for account in credit_accounts:
        enhanced = account.copy()
        
        # Classify repayment status for active accounts
        is_settled = is_settled_account(account)
        
        if is_settled:
            # Classify settled loan status
            risk_class = account.get("credit_account_risk_classification", "")
            product_type = account.get("product_type", "")
            settled_status = classify_settled_loan_status(f"{risk_class} {product_type}")
            enhanced["account_status"] = "Settled"
            enhanced["settled_loan_status"] = settled_status
            
            # Count by settled status
            settled_loans_count[settled_status] = settled_loans_count.get(settled_status, 0) + 1
        else:
            # Classify repayment status for active loans
            risk_class = account.get("credit_account_risk_classification", "")
            payment_due = account.get("payment_due_date", "")
            repayment_status = classify_repayment_status(risk_class, payment_due)
            enhanced["account_status"] = "Active"
            enhanced["repayment_status"] = repayment_status
            
            # Count by repayment status
            repayment_status_count[repayment_status] = repayment_status_count.get(repayment_status, 0) + 1
        
        enhanced_accounts.append(enhanced)

    tl_rows = coopbank_term_loan_scores_for_accounts(enhanced_accounts)["per_account"]
    for acc, row in zip(enhanced_accounts, tl_rows):
        acc["coopbank_performance_bucket"] = row["coopbank_performance_bucket"]
        acc["coopbank_term_loan_decimal"] = row["coopbank_term_loan_decimal"]

    # Calculate credit exposure
    credit_exposure = calculate_credit_exposure(credit_accounts)
    
    # Convert CreditExposure dataclass to dict for serialization
    exposure_dict = {
        "total_approved_amount": credit_exposure.total_approved_amount,
        "total_current_balance": credit_exposure.total_current_balance,
        "total_outstanding": credit_exposure.total_outstanding,
        "total_by_risk_classification": credit_exposure.total_by_risk_classification,
        "total_by_product_type": credit_exposure.total_by_product_type,
        "total_by_institution": credit_exposure.total_by_institution,
        "active_accounts_count": credit_exposure.active_accounts_count,
        "settled_accounts_count": credit_exposure.settled_accounts_count
    }

    # Facility-type aggregates + exposure table rows (for Streamlit/API)
    by_facility_type: Dict[str, Dict[str, float]] = {}
    exposure_table_rows: List[Dict[str, Any]] = []

    for acc in enhanced_accounts:
        facility_type = classify_facility_type(acc)
        acc["facility_type"] = facility_type

        approved = parse_amount(acc.get("approved_amount", ""))
        current_balance = parse_amount(acc.get("current_balance_amount", ""))
        status = acc.get("account_status", "Unknown")

        bucket = by_facility_type.setdefault(
            facility_type,
            {
                "approved_amount_total_all": 0.0,
                "current_balance_total_all": 0.0,
                "approved_amount_total_active": 0.0,
                "current_balance_total_active": 0.0,
                "accounts_count_all": 0.0,
                "accounts_count_active": 0.0,
            },
        )

        bucket["approved_amount_total_all"] += approved
        bucket["current_balance_total_all"] += current_balance
        bucket["accounts_count_all"] += 1.0

        if status == "Active":
            bucket["approved_amount_total_active"] += approved
            bucket["current_balance_total_active"] += current_balance
            bucket["accounts_count_active"] += 1.0

        exposure_table_rows.append(
            {
                "institution": acc.get("institution", ""),
                "lending_bank": acc.get("institution", ""),
                "date_granted": acc.get("date_account_opened", ""),
                "expiry_date": acc.get("maturity_date", ""),
                "credit_product": acc.get("product_type", ""),
                "facility_type": facility_type,
                "amount_granted": approved,
                "total_current_balance": current_balance,
                "status": status,
                "account_number": acc.get("account_number", ""),
            }
        )

    exposure_dict["by_facility_type"] = by_facility_type
    exposure_dict["exposure_table_rows"] = exposure_table_rows

    coopbank_cic = coopbank_cic_credit_history_automation(
        enhanced_accounts,
        credit_exposure.total_approved_amount,
        aggregation="mean",
    )
    
    result = {
        "enhanced_accounts": enhanced_accounts,
        "repayment_status_breakdown": repayment_status_count,
        "settled_loans_breakdown": settled_loans_count,
        "credit_exposure": exposure_dict,
        "coopbank_grading": {
            "credit_history_cic_automated": coopbank_cic,
            "reference": "CoopBank Risk grading for SMEs.xlsx — Credit History (CIC)",
        },
        "total_accounts": len(credit_accounts),
        "active_accounts": credit_exposure.active_accounts_count,
        "settled_accounts": credit_exposure.settled_accounts_count
    }
    
    logger.info(f"✅ Credit analysis complete: {credit_exposure.active_accounts_count} active, {credit_exposure.settled_accounts_count} settled")
    
    return result


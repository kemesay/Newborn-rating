import json
import os
import re
import argparse
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None

try:
    from groq import Groq  # type: ignore
except Exception:  # pragma: no cover
    Groq = None  # type: ignore


DEFAULT_INPUT_JSON_PATH = \
    "/home/name-1/AI-Agent/new-born/data/raw/financial_lines_psm6.json"

DEFAULT_OUTPUT_BALANCE_SHEET_PATH = \
    "/home/name-1/AI-Agent/new-born/data/raw/extracted_balance_sheet_2024.json"

DEFAULT_OUTPUT_INCOME_STATEMENT_PATH = \
    "/home/name-1/AI-Agent/new-born/data/raw/extracted_income_statement_2024.json"


HEADING_BALANCE_SHEET_PATTERNS: List[re.Pattern] = [
    re.compile(r"\bstatement\s+of\s+financial\s+position\b", re.IGNORECASE),
    re.compile(r"\bbalance\s*sheet\b", re.IGNORECASE),
    re.compile(r"\bfinancial\s+position\b", re.IGNORECASE),
    re.compile(r"\bstatement\s+of\s+financial\s+condition\b", re.IGNORECASE),
    re.compile(r"\bstatement\s+of\s+assets?,?\s*liabilities\s*(and|&)\s*equity\b", re.IGNORECASE),
    re.compile(r"\bstatement\s+of\s+financial\s+status\b", re.IGNORECASE),
]


HEADING_INCOME_STATEMENT_PATTERNS: List[re.Pattern] = [
    re.compile(
        r"\bstatement\s+of\s+profit\s+or\s+loss(\s+and\s+other\s+comprehensive\s+income)?\b",
        re.IGNORECASE,
    ),
    # Variants observed in some documents
    re.compile(r"\bstatement\s+of\s+profit\s+or\s+loss\s+and\s+another\s+comprehensive\s+income\b", re.IGNORECASE),
    re.compile(r"\bstatement\s+of\s+profit\s+or\s+loss\s+and\s+comprehensive\s+income\b", re.IGNORECASE),
    re.compile(r"\bincome\s+statement\b", re.IGNORECASE),
    re.compile(r"\bstatement\s+of\s+income\b", re.IGNORECASE),
    re.compile(r"\bstatement\s+of\s+operations\b", re.IGNORECASE),
    re.compile(r"\bprofit\s+and\s+loss\b", re.IGNORECASE),
    re.compile(r"\bp\s*&\s*l\b", re.IGNORECASE),
    re.compile(r"\bprofit\s+and\s+loss\s+account\b", re.IGNORECASE),
    re.compile(r"\bstatement\s+of\s+earnings\b", re.IGNORECASE),
]


NEXT_SECTION_BOUNDARY_PATTERNS: List[re.Pattern] = [
    re.compile(r"statement\s+of\s+profit\s+or\s+loss", re.IGNORECASE),
    re.compile(r"statement\s+of\s+change[s]?\s+in\s+equity", re.IGNORECASE),
    re.compile(r"statement\s+of\s+cash\s+flows", re.IGNORECASE),
    re.compile(r"notes?\s+to\s+the\s+financial\s+statements", re.IGNORECASE),
]


# Accept numbers with comma group separators (e.g., 1,234,567.89) and plain numbers (e.g., 1234567.89).
# Do NOT treat spaces inside digits as grouping (to avoid merging separate values like years/notes).
AMOUNT_TOKEN_PATTERN = r"\(?-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?\)?"
AMOUNT_PATTERN = re.compile(AMOUNT_TOKEN_PATTERN)
TRAILING_VALUES_PATTERN = re.compile(
    rf"({AMOUNT_TOKEN_PATTERN}|[-—–]|0)\s+({AMOUNT_TOKEN_PATTERN}|[-—–]|0)\s*$"
)
DASH_TOKENS = {"-", "—", "–"}
SINGLE_TRAILING_DASH_PATTERN = re.compile(r"[-—–]\s*$")
DASH_SMALLINT_TAIL = re.compile(r"[-—–]\s+(\d{1,2})\s*$")
SMALLINT_DASH_TAIL = re.compile(r"(\d{1,2})\s+[-—–]\s*$")

# Month/date helpers and heuristics
MONTH_NAME_PATTERN = re.compile(
    r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec|january|february|march|april|june|july|august|september|october|november|december)\b",
    re.IGNORECASE,
)
FIN_KEYWORDS = {
    "assets", "liabilities", "equity", "revenue", "expense", "expenses",
    "profit", "loss", "cash", "payable", "receivable", "comprehensive",
}

def _is_header_like_text(text: str) -> bool:
    t = text.strip().lower()
    if not t:
        return False
    if t.startswith("as at") or t.startswith("as of"):
        return True
    if t.startswith("note") or t.startswith("notes"):
        return True
    if "for the year ended" in t:
        return True
    if MONTH_NAME_PATTERN.search(t) and not any(k in t for k in FIN_KEYWORDS):
        return True
    return False


def initialize_env() -> None:
    if load_dotenv is not None:
        try:
            load_dotenv()
        except Exception:
            pass


def get_groq_client() -> Optional[Any]:
    if Groq is None:
        return None
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        return None
    try:
        return Groq(api_key=api_key)
    except Exception:
        return None


def clean_label_text(text: str) -> str:
    original = text
    t = text.strip()
    # Remove leading bullet/noise tokens and common OCR prefixes
    t = re.sub(r"^\W+", "", t)
    # Strip known small prefixes like 'i]', 'q', 'a', 'F]', 'F|', 'Fl', 'nf', leading underscores, stray letters
    t = re.sub(r"^(?:i\]|q|a|p|n|F\]|F\||Fl|nf|_+)\s+", "", t, flags=re.IGNORECASE)
    # Strip single-letter prefixes like 'T ' before totals
    t = re.sub(r"^[A-Za-z]\s+Total\b", "Total", t)
    # Also strip repeated occurrences of such noise
    t = re.sub(r"^(?:[qaivf]{1,2}\]?\)?\|?\.?\s+)+", "", t, flags=re.IGNORECASE)
    # Fix common OCR mistakes
    replacements = {
        "trom": "from",
        "laiblities": "liabilities",
        "liablities": "liabilities",
        "Right use of assets": "Right-of-use assets",
        "Right use": "Right-of-use",
        "Investment in Equity": "Investments in equity",
        "Investments in Equity": "Investments in equity",
        "Fl Investments in equity": "Investments in equity",
        "Other reserve result from adoptiono": "Other reserves",
        "Retained earnings SCE": "Retained earnings",
        "Lease payable 13, ii": "Lease payable",
        "Trade and other receivables iS.": "Trade and other receivables",
        "Cash and cash equivalents i.": "Cash and cash equivalents",
        "Other operating income th": "Other operating income",
        "and and": "and",
        "expenses expenses": "expenses",
    }
    for k, v in replacements.items():
        t = t.replace(k, v)
    # Drop trailing note-like tokens (e.g., '12.2.', '13,i', 'i', 'ii', etc.)
    tokens = t.split()
    while tokens:
        last = tokens[-1]
        if NOTE_TOKEN_PATTERN.match(last) or re.match(r"\d{1,2}[,\.]?i?", last) or re.fullmatch(r"i{1,3}", last, flags=re.IGNORECASE):
            tokens.pop()
            continue
        # strip trailing punctuation-only tokens
        if re.fullmatch(r"[\.,;:]+", last):
            tokens.pop()
            continue
        break
    t = " ".join(tokens)
    # Drop stray repeated punctuation
    t = re.sub(r"\s*[\.|,;:]+\s*$", "", t)
    # Strip trailing noise characters (e.g., "Jogi a> ’")
    t = re.sub(r"[^A-Za-z0-9()&\s]+$", "", t)
    # Canonicalize common totals
    t = re.sub(r"^Total\s+assets.*$", "Total assets", t, flags=re.IGNORECASE)
    t = re.sub(r"^Total\s+equity\s+and\s+liabilities.*$", "Total equity and liabilities", t, flags=re.IGNORECASE)
    t = re.sub(r"^Total\s+equity.*$", "Total equity", t, flags=re.IGNORECASE)
    # Normalize spacing and case (keep original capitalization mostly)
    t = normalize_space(t)
    if not t:
        return original.strip()
    return t


def is_plausible_label(description: str, statement_type: str) -> bool:
    # Reject trivial or obviously noisy labels
    dl = description.lower().strip()
    if dl in {"s", "a", "t", "pi", "nf", "l"}:
        return False
    if not re.search(r"[a-zA-Z]", description):
        return False
    # Reject very long narrative-like lines unless they contain key financial words
    words = description.split()
    if len(words) > 12 and not any(
        kw in dl
        for kw in [
            "assets",
            "liabilities",
            "equity",
            "revenue",
            "expense",
            "profit",
            "loss",
            "comprehensive",
            "payable",
            "receivable",
            "cash",
        ]
    ):
        return False
    # Exclude known noisy phrases
    noisy_phrases = [
        "statement of",
        "for the year ended",
        "currency",
        "habesha",
        "products",
        "distributer",
        "change in equity",
        "cash flow",
    ]
    if any(p in dl for p in noisy_phrases):
        return False
    return True


def normalize_group_name(group: Optional[str]) -> Optional[str]:
    if not group:
        return group
    g = group.lower()
    if "non-current assets" in g or ("assets" in g and "non-current" in g):
        return "Non-current assets"
    if "current assets" in g:
        return "Current assets"
    if "equity and liabilities" in g:
        return "Equity and liabilities"
    if "equity" in g:
        return "Equity"
    if "non-current liabilities" in g or ("liabilities" in g and "non-current" in g):
        return "Non-current liabilities"
    if "current liabilities" in g:
        return "Current liabilities"
    if "other comprehensive income" in g:
        return "Other comprehensive income"
    if "revenue" in g:
        return "Revenue"
    if "expenses" in g:
        return "Expenses"
    if "profit" in g or "loss" in g:
        return "Profit or loss"
    return normalize_space(group)


def post_process_with_groq(
    client: Any, statement: Dict[str, Any], statement_type: str
) -> Dict[str, Any]:
    # Build compact payload to limit token usage
    items_brief = [
        {
            "label": i.get("label"),
            "group": i.get("group"),
        }
        for i in statement.get("items", [])
    ]

    system_msg = (
        "You clean OCR noisy financial statement line items. "
        "Return only JSON array matching the input order, each element with: "
        "label_clean, group_clean. Use IFRS terminology. Do not invent or remove lines."
        "if the label is not a financial term, then do not change it"
        "if you get - replace that with 0"
        "make sure that the amount is not negative"
        "make sure removing '()' from the amount and don ever replace with negative"
        "on the periods make sure that the mapping of date formatting it may yy-mm-dd or dd-mm-yy and also the year may shortened 2020 may shown as 20"
    )
    user_msg = (
        f"Statement type: {statement_type}. Clean and normalize labels and groups.\n"
        f"Input JSON: {json.dumps(items_brief, ensure_ascii=False)}\n"
        "Output strictly as JSON array with objects: {label_clean, group_clean}."
    )

    try:
        resp = client.chat.completions.create(
            model=os.environ.get("GROQ_MODEL", "llama-3.1-8b-instant"),
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content  # type: ignore
        # Expecting a JSON object, but we asked for array; wrap accordingly
        # Try to extract array from the content
        parsed = json.loads(content)
        # Accept either { "items": [...] } or just [...]
        cleaned_items = parsed.get("items") if isinstance(parsed, dict) else parsed
        if not isinstance(cleaned_items, list):
            return statement
        # If the model returned a list with different length, avoid truncating/expanding: keep original length
        original_items = statement.get("items", [])
        merged_items: List[Dict[str, Any]] = []
        for idx, orig in enumerate(original_items):
            merged = dict(orig)
            if idx < len(cleaned_items) and isinstance(cleaned_items[idx], dict):
                label_clean = cleaned_items[idx].get("label_clean")
                group_clean = cleaned_items[idx].get("group_clean")
                if label_clean:
                    merged["label"] = label_clean
                if group_clean:
                    merged["group"] = group_clean
            merged_items.append(merged)
        statement["items"] = merged_items
        return statement
    except Exception:
        return statement


def finalize_totals_and_groups(statement: Dict[str, Any]) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = statement.get("items", [])
    # 1) Canonicalize totals and fix groups based on label
    for it in items:
        label = it.get("label", "")
        low = label.lower()
        if low.startswith("total assets"):
            it["label"] = "Total assets"
            it["group"] = "Assets"
        elif low.startswith("total equity and liabilities"):
            it["label"] = "Total equity and liabilities"
            it["group"] = "Equity and liabilities"
        elif low.startswith("total liabilities"):
            it["label"] = "Total liabilities"
            it["group"] = "Liabilities"
        elif low.startswith("total equity"):
            it["label"] = "Total equity"
            it["group"] = "Equity"

    # 2) If we have Total assets, and a line labeled Total equity equals that amount, rename it to Total equity and liabilities
    total_assets_current = None
    total_assets_prior = None
    for it in items:
        if it.get("label", "").lower().startswith("total assets"):
            total_assets_current = it.get("amount_current")
            total_assets_prior = it.get("amount_prior")
            break
    if total_assets_current is not None and total_assets_prior is not None:
        for it in items:
            if it.get("label", "").lower().startswith("total equity") and not it.get("label", "").lower().startswith("total equity and liabilities"):
                ac = it.get("amount_current")
                ap = it.get("amount_prior")
                # compare within a tiny tolerance to avoid float issues
                if ac == total_assets_current and ap == total_assets_prior:
                    it["label"] = "Total equity and liabilities"
                    it["group"] = "Equity and liabilities"
    statement["items"] = items

    # 3) Truncate any trailing noise after the final total
    final_idx: Optional[int] = None
    for idx, it in enumerate(items):
        if it.get("label", "").lower().startswith("total equity and liabilities"):
            final_idx = idx
            break
    if final_idx is not None:
        items = items[: final_idx + 1]

    # 4) Drop trivial/noise items that slipped through
    filtered: List[Dict[str, Any]] = []
    for it in items:
        label = (it.get("label") or "").strip()
        if len(label) <= 2 and label.lower() in {"s", "a", "t", "l", "pi", "nf"}:
            continue
        filtered.append(it)

    statement["items"] = filtered
    return statement
NOTE_TOKEN_PATTERN = re.compile(r"^\d{1,2}(?:\.\d+)?\.?$")


def load_lines(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_space(text: str) -> str:
    # Collapse unicode spaces and stray control characters
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def line_matches_any(text: str, patterns: List[re.Pattern]) -> bool:
    return any(p.search(text) is not None for p in patterns)


def find_section_bounds(
    lines: List[Dict[str, Any]], start_patterns: List[re.Pattern]
) -> Optional[Tuple[int, int]]:
    """
    Score potential heading candidates and choose the most likely table header,
    avoiding narrative mentions. Heuristics:
      - Short heading line (<= 12 words)
      - Nearby context mentions 'Currency' or 'Notes'
      - Nearby context includes 'AS AT' or 'FOR THE YEAR ENDED'
      - Many numeric lines with two amounts within the following window
    """
    candidate_indices: List[int] = []
    for idx, entry in enumerate(lines):
        line_text = normalize_space(entry.get("line", ""))
        if line_matches_any(line_text, start_patterns):
            # Filter out narrative-like lines
            words = line_text.split()
            if len(words) > 12:
                continue
            if "comprise" in line_text.lower():
                continue
            candidate_indices.append(idx)

    if not candidate_indices:
        return None

    def score_candidate(anchor: int) -> int:
        score = 0
        line_text = normalize_space(lines[anchor].get("line", "")).lower()
        
        # Strongly prefer exact statement titles over narrative mentions
        if line_text.strip() in ["statement of financial position", "balance sheet"]:
            score += 100
        elif any(phrase in line_text for phrase in ["statement of financial position", "balance sheet"]):
            # If it contains but is not exactly the title, it's likely narrative
            if len(line_text.split()) > 8:  # Long line = narrative
                score -= 50
            else:
                score += 20
        
        window_after = [normalize_space(lines[i].get("line", "")) for i in range(anchor, min(anchor + 80, len(lines)))]
        joined = "\n".join(window_after).lower()
        if "currency" in joined:
            score += 10
        if "notes" in joined:
            score += 10
        if "as at" in joined:
            score += 15
        if "for the year ended" in joined:
            score += 10
        
        # Count numeric lines with at least two amounts
        numeric_lines = 0
        for wline in window_after:
            if len(AMOUNT_PATTERN.findall(wline)) >= 2:
                numeric_lines += 1
        score += min(numeric_lines, 30)  # cap contribution
        
        # Prefer pages 5-15 (typical financial statement location)
        page = lines[anchor].get("page")
        try:
            page_num = int(page)
            if 5 <= page_num <= 15:
                score += 10
            elif page_num > 20:
                score -= 20  # Likely notes section
        except Exception:
            pass
        return score

    best_idx = max(candidate_indices, key=score_candidate)

    # Determine section end by next section boundary after best_idx.
    # Additionally, stop at visual separators to prevent bleed (e.g., the next statement's title block lines).
    end_index: Optional[int] = None
    hard_boundaries = NEXT_SECTION_BOUNDARY_PATTERNS
    soft_boundaries = [
        re.compile(r"^\s*STATEMENT\b", re.IGNORECASE),
        re.compile(r"^\s*FOR THE YEAR ENDED\b", re.IGNORECASE),
        re.compile(r"^\s*Currency\b", re.IGNORECASE),
        re.compile(r"^\s*NOTES?\b", re.IGNORECASE),
        re.compile(r"^\s*HABESHA\b", re.IGNORECASE),
    ]
    for idx in range(best_idx + 1, len(lines)):
        line_text = normalize_space(lines[idx].get("line", ""))
        if line_matches_any(line_text, hard_boundaries):
            end_index = idx
            break
        # Only consider soft boundaries after a minimum distance to avoid cutting within the header block
        if idx - best_idx >= 8 and line_matches_any(line_text, soft_boundaries):
            end_index = idx
            break

    if end_index is None:
        end_index = len(lines)

    return best_idx, end_index


def parse_amount(token: str) -> Optional[float]:
    token = token.strip()
    if token in DASH_TOKENS:
        return 0.0
    if token == "":
        return None
    negative = token.startswith("(") and token.endswith(")")
    cleaned = token.replace("(", "").replace(")", "").replace(",", "")
    try:
        value = float(cleaned)
        if negative:
            value = -value
        return value
    except ValueError:
        return None


def extract_periods(header_lines: List[str]) -> Tuple[Optional[str], Optional[str]]:
    # Look for date patterns like: "Notes 30-Jun-24 30-Jun-23" or "30-Jun-2021 30-Jun-20"
    for header in header_lines:
        # Try full 4-digit years first
        full_matches = re.findall(r"\b\d{1,2}[-/][A-Za-z]{3}[-/]\d{4}\b", header)
        if len(full_matches) >= 2:
            return full_matches[0], full_matches[1]
        
        # Try 2-digit years
        short_matches = re.findall(r"\b\d{1,2}[-/][A-Za-z]{3}[-/]\d{2}\b", header)
        if len(short_matches) >= 2:
            return short_matches[0], short_matches[1]
        
        # Mixed format: one 4-digit, one 2-digit (like "30-Jun-2021 30-Jun-20")
        mixed_pattern = r"\b(\d{1,2}[-/][A-Za-z]{3}[-/]\d{2,4})\s+(\d{1,2}[-/][A-Za-z]{3}[-/]\d{2,4})\b"
        mixed_match = re.search(mixed_pattern, header)
        if mixed_match:
            return mixed_match.group(1), mixed_match.group(2)
    
    return None, None


def parse_statement_items(
    section_lines: List[str],
    statement_type: str,
) -> Dict[str, Any]:
    header_candidates: List[str] = []
    items: List[Dict[str, Any]] = []
    current_group: Optional[str] = None

    for raw_line in section_lines:
        line = normalize_space(raw_line)
        if not line:
            continue

        # Capture header candidates for period extraction
        if (
            "note" in line.lower()
            or re.search(r"\b\d{1,2}[-/][A-Za-z]{3}[-/]\d{2,4}\b", line, re.IGNORECASE)
            or "currency" in line.lower()
        ):
            header_candidates.append(line)

        # Identify grouping headers
        is_potential_group = (
            # Lines without amounts are likely headers
            AMOUNT_PATTERN.search(line) is None
            and len(line.split()) <= 6
        )

        if is_potential_group:
            # Accept typical group labels
            if any(
                kw in line.lower()
                for kw in [
                    "assets",
                    "liabilities",
                    "equity",
                    "current assets",
                    "non-current assets",
                    "current liabilities",
                    "non-current liabilities",
                    "equity and liabilities",
                    "revenue",
                    "expenses",
                    "other comprehensive income",
                    "comprehensive income",
                    "profit",
                    "loss",
                ]
            ):
                current_group = line
                continue

        # Parse lines with amounts
        amount_tokens = AMOUNT_PATTERN.findall(line)
        if len(amount_tokens) >= 2:
            amount_current = parse_amount(amount_tokens[-2])
            amount_prior = parse_amount(amount_tokens[-1])

            # Description: remove all matched numeric tokens
            desc_part = AMOUNT_PATTERN.sub(" ", line)
            desc_part = normalize_space(desc_part)

            # Extract optional trailing note token from description
            tokens = desc_part.split()
            note_value: Optional[str] = None
            if tokens:
                last_token = tokens[-1]
                if NOTE_TOKEN_PATTERN.match(last_token):
                    note_value = last_token
                    tokens = tokens[:-1]
            description = normalize_space(" ".join(tokens))

            # Drop overly short or noisy descriptions and date/header-like lines
            if (
                len(description) <= 1
                or not is_plausible_label(description, statement_type)
                or _is_header_like_text(description)
            ):
                continue

            # If both parsed amounts are very small (<= 20), likely not values (years/notes). Skip unless label has strong financial keywords
            if (
                isinstance(amount_current, (int, float))
                and isinstance(amount_prior, (int, float))
                and abs(float(amount_current)) <= 20
                and abs(float(amount_prior)) <= 20
                and not any(k in description.lower() for k in FIN_KEYWORDS)
            ):
                continue

            # Remove any residual small integers (likely note refs/years) from the description
            description = normalize_space(re.sub(r"\b\d{1,2}\b", "", description))
            if len(description) <= 1:
                continue

            item: Dict[str, Any] = {
                "label": description,
                "note": note_value,
                "amount_current": amount_current,
                "amount_prior": amount_prior,
            }
            if current_group:
                item["group"] = current_group
            items.append(item)

            # Early stop if we reached the known final total for the statement
            desc_low = description.lower()
            if statement_type == "balance_sheet" and desc_low.startswith("total equity and liabilities"):
                break
            if statement_type == "income_statement" and (
                desc_low.startswith("total comprehensive income for the year")
                or desc_low.startswith("profit after tax for the year")
            ):
                # Some IS don't include OCI; stop at profit after tax if no OCI is present
                # We do not break immediately on first occurrence; ensure it's the last logical subtotal
                # In practice, stopping here prevents bleed from next statements
                pass
            continue

        # Fallback: detect trailing two values (including dashes) at end of line
        m = TRAILING_VALUES_PATTERN.search(line)
        if m:
            tok_current, tok_prior = m.group(1), m.group(2)
            amount_current = parse_amount(tok_current)
            amount_prior = parse_amount(tok_prior)

            # Description: part before the matched trailing values
            desc_part = normalize_space(line[: m.start()])

            # Remove optional trailing note token from description
            tokens = desc_part.split()
            note_value: Optional[str] = None
            if tokens:
                last_token = tokens[-1]
                if NOTE_TOKEN_PATTERN.match(last_token):
                    note_value = last_token
                    tokens = tokens[:-1]
            description = normalize_space(" ".join(tokens))

            if (
                len(description) <= 1
                or not is_plausible_label(description, statement_type)
                or _is_header_like_text(description)
            ):
                continue

            if (
                isinstance(amount_current, (int, float))
                and isinstance(amount_prior, (int, float))
                and abs(float(amount_current)) <= 20
                and abs(float(amount_prior)) <= 20
                and not any(k in description.lower() for k in FIN_KEYWORDS)
            ):
                continue

            description = normalize_space(re.sub(r"\b\d{1,2}\b", "", description))
            if len(description) <= 1:
                continue

            item = {
                "label": description,
                "note": note_value,
                "amount_current": amount_current if amount_current is not None else 0.0,
                "amount_prior": amount_prior if amount_prior is not None else 0.0,
            }
            if current_group:
                item["group"] = current_group
            items.append(item)

            desc_low = description.lower()
            if statement_type == "balance_sheet" and desc_low.startswith("total equity and liabilities"):
                break
            if statement_type == "income_statement" and (
                desc_low.startswith("total comprehensive income for the year")
                or desc_low.startswith("profit after tax for the year")
            ):
                pass
            continue

        # Fallback 2: single trailing dash -> treat both periods as 0
        if SINGLE_TRAILING_DASH_PATTERN.search(line):
            desc_part = normalize_space(SINGLE_TRAILING_DASH_PATTERN.sub("", line))
            tokens = desc_part.split()
            note_value: Optional[str] = None
            if tokens:
                last_token = tokens[-1]
                if NOTE_TOKEN_PATTERN.match(last_token):
                    note_value = last_token
                    tokens = tokens[:-1]
            description = normalize_space(" ".join(tokens))

            if len(description) <= 1 or not is_plausible_label(description, statement_type):
                continue

            item = {
                "label": description,
                "note": note_value,
                "amount_current": 0.0,
                "amount_prior": 0.0,
            }
            if current_group:
                item["group"] = current_group
            items.append(item)

            desc_low = description.lower()
            if statement_type == "balance_sheet" and desc_low.startswith("total equity and liabilities"):
                break
            if statement_type == "income_statement" and (
                desc_low.startswith("total comprehensive income for the year")
                or desc_low.startswith("profit after tax for the year")
            ):
                pass
            continue

        # Fallback 3: dash + small int noise (e.g., "- 7") or small int + dash
        if DASH_SMALLINT_TAIL.search(line) or SMALLINT_DASH_TAIL.search(line):
            m = DASH_SMALLINT_TAIL.search(line) or SMALLINT_DASH_TAIL.search(line)
            assert m is not None
            desc_part = normalize_space(line[: m.start()])
            tokens = desc_part.split()
            note_value = None
            if tokens:
                last_token = tokens[-1]
                if NOTE_TOKEN_PATTERN.match(last_token):
                    note_value = last_token
                    tokens = tokens[:-1]
            description = normalize_space(" ".join(tokens))
            if len(description) <= 1:
                continue
            item = {
                "label": description,
                "note": note_value,
                "amount_current": 0.0,
                "amount_prior": 0.0,
            }
            if current_group:
                item["group"] = current_group
            items.append(item)

    period_current, period_prior = extract_periods(header_candidates)
    # Clean labels and normalize groups locally
    for it in items:
        cleaned_label = clean_label_text(it.get("label", ""))
        it["label"] = cleaned_label
        if "group" in it:
            it["group"] = normalize_group_name(it.get("group"))
        # Adjust groups for totals
        low = cleaned_label.lower()
        if low.startswith("total "):
            if low.startswith("total assets"):
                it["group"] = "Assets"
            elif low.startswith("total equity and liabilities"):
                it["group"] = "Equity and liabilities"
            elif low.startswith("total equity"):
                it["group"] = "Equity"
            else:
                it["group"] = "Totals"

    return {
        "period_current": period_current,
        "period_prior": period_prior,
        "items": items,
    }


def extract_statements_from_entries(
    raw_entries: List[Dict[str, Any]]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    initialize_env()
    groq_client = get_groq_client()

    lines = [normalize_space(entry.get("line", "")) for entry in raw_entries]

    # First pass: search in the first half of the document to avoid scanning notes
    half_index = max(1, len(raw_entries) // 2)
    first_half_entries = raw_entries[:half_index]
    bs_bounds = find_section_bounds(first_half_entries, HEADING_BALANCE_SHEET_PATTERNS)
    is_bounds = find_section_bounds(first_half_entries, HEADING_INCOME_STATEMENT_PATTERNS)
    print(f"BS bounds-----: {bs_bounds}")
    print(f"IS bounds-----: {is_bounds}")

    # If not found in first half, fall back to full document
    if bs_bounds is not None:
        bs_bounds = (bs_bounds[0], bs_bounds[1])
    else:
        bs_bounds = find_section_bounds(raw_entries, HEADING_BALANCE_SHEET_PATTERNS)

    # For OCR like 2023, IS title may be garbled. Fallback: locate the 'Notes <date> <date>' row nearest after BS
    if is_bounds is not None:
        is_bounds = (is_bounds[0], is_bounds[1])
    else:
        is_bounds = find_section_bounds(raw_entries, HEADING_INCOME_STATEMENT_PATTERNS)
    if is_bounds is None:
        # Find all Note headers; choose the one closest after the BS end as IS header, but before Cash Flow
        date_header_regex = re.compile(r"^Notes?\s+\d{1,2}-[A-Za-z]{3}-\d{2,4}\s+\d{1,2}-[A-Za-z]{3}-\d{2,4}$", re.IGNORECASE)
        note_indices: List[int] = [idx for idx, e in enumerate(raw_entries) if date_header_regex.match(normalize_space(e.get("line", "")))]
        candidate = None
        if bs_bounds is not None:
            bs_end_idx = bs_bounds[1]
            for idx in note_indices:
                if idx > bs_end_idx:
                    candidate = idx
                    break
        else:
            # If BS not found, fall back to the first plausible date header
            if note_indices:
                candidate = note_indices[0]
        if candidate is not None:
            # Walk back a few lines to include the title lines and currency
            start_idx = max(0, candidate - 6)
            # End at next hard boundary or page change
            end_idx = None
            for j in range(candidate + 1, min(candidate + 120, len(raw_entries))):
                t = normalize_space(raw_entries[j].get("line", ""))
                if line_matches_any(t, NEXT_SECTION_BOUNDARY_PATTERNS) or t.lower().startswith("statement of cash"):
                    end_idx = j
                    break
            if end_idx is None:
                end_idx = min(candidate + 120, len(raw_entries))
            is_bounds = (start_idx, end_idx)

    # Conditional processing: only process statements that exist
    balance_sheet = {"period_current": None, "period_prior": None, "items": []}
    income_statement = {"period_current": None, "period_prior": None, "items": []}

    # Process Balance Sheet if found
    if bs_bounds is not None:
        bs_start, bs_end = bs_bounds
        print(f"BS start-----: {bs_start}, BS end-----: {bs_end}")
        try:
            bs_page = raw_entries[bs_start].get("page")
            print(f"Detected BS start index {bs_start} (page {bs_page}), end {bs_end}")
            # Debug: show first few lines of BS section
            print("BS lines sample:")
            for i in range(bs_start, min(bs_start + 5, bs_end)):
                print(f"  {i}: {raw_entries[i]['line'][:80]}")
        except Exception:
            pass

        balance_sheet_lines = [raw_entries[i]["line"] for i in range(bs_start, bs_end)]
        balance_sheet = parse_statement_items(balance_sheet_lines, "balance_sheet")
        print(f"Balance sheet before groq: {balance_sheet}")
        if groq_client is not None:
            balance_sheet = post_process_with_groq(groq_client, balance_sheet, "balance_sheet")
        print(f"Balance sheet after groq: {balance_sheet}")
        balance_sheet = finalize_totals_and_groups(balance_sheet)
        print(f"Balance sheet after totals: {balance_sheet}")
    else:
        print("⚠️ Balance Sheet (Statement of Financial Position) section not found. Skipping...")

    # Process Income Statement if found
    if is_bounds is not None:
        is_start, is_end = is_bounds
        print(f"IS start-----: {is_start}, IS end-----: {is_end}")
        try:
            is_page = raw_entries[is_start].get("page")
            print(f"Detected IS start index {is_start} (page {is_page}), end {is_end}")
            # Debug: show first few lines of IS section
            print("IS lines sample:")
            for i in range(is_start, min(is_start + 5, is_end)):
                print(f"  {i}: {raw_entries[i]['line'][:80]}")
        except Exception:
            pass

        income_statement_lines = [raw_entries[i]["line"] for i in range(is_start, is_end)]
        income_statement = parse_statement_items(income_statement_lines, "income_statement")
        print(f"Income statement before groq: {income_statement}")
        if groq_client is not None:
            income_statement = post_process_with_groq(groq_client, income_statement, "income_statement")
        print(f"Income statement after groq: {income_statement}")
        income_statement = finalize_totals_and_groups(income_statement)
        print(f"Income statement after totals: {income_statement}")
    else:
        print("⚠️ Income Statement (Statement of Profit or Loss) section not found. Skipping...")

    return balance_sheet, income_statement


def extract_statements(input_json_path: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    raw_entries = load_lines(input_json_path)
    return extract_statements_from_entries(raw_entries)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract balance sheet and income statement from OCR lines JSON.")
    parser.add_argument("--input-json", default=DEFAULT_INPUT_JSON_PATH, help="Path to OCR lines JSON input")
    parser.add_argument("--output-balance-json", default=DEFAULT_OUTPUT_BALANCE_SHEET_PATH, help="Path to write balance sheet JSON")
    parser.add_argument("--output-income-json", default=DEFAULT_OUTPUT_INCOME_STATEMENT_PATH, help="Path to write income statement JSON")
    args = parser.parse_args()

    balance_sheet, income_statement = extract_statements(args.input_json)

    os.makedirs(os.path.dirname(args.output_balance_json), exist_ok=True)
    with open(args.output_balance_json, "w", encoding="utf-8") as f:
        json.dump(balance_sheet, f, ensure_ascii=False, indent=2)

    os.makedirs(os.path.dirname(args.output_income_json), exist_ok=True)
    with open(args.output_income_json, "w", encoding="utf-8") as f:
        json.dump(income_statement, f, ensure_ascii=False, indent=2)

    print(
        f"Wrote balance sheet to: {args.output_balance_json}\n"
        f"Wrote income statement to: {args.output_income_json}"
    )


if __name__ == "__main__":
    main()



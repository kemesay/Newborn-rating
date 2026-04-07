import io
import os
import re
import time
import zipfile
import tempfile
import logging
from typing import Any, Dict, List, Tuple, Set, Callable, Optional

import pandas as pd
import streamlit as st

# ============================================================================
# FINANCIAL STATEMENTS MODULES (Balance Sheet & Income Statement)
# ============================================================================
from ocr_to_lines import ocr_pdf_to_lines                    # ✅ Financial Statements: OCR processing
from extract_statements import extract_statements_from_entries  # ✅ Financial Statements: Statement extraction

# ============================================================================
# CIC DOCUMENTS MODULES (Credit Information Center)
# ============================================================================
from cic_extractor import extract_cic_from_pdf                # ✅ CIC Documents: Credit account extraction
from credit_rating_analyzer import analyze_credit_accounts     # ✅ CIC Documents: Credit rating analysis
from coopbank_sme_grading import (
    compute_financial_position_40,
    management_capacity_9,
    integrity_6,
    banking_relationship_10,
    industry_attractiveness_10,
    interpret_business_risk_grade,
)

# Configure logging for Streamlit
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ============================================================================
# FINANCIAL STATEMENTS FUNCTIONS (Balance Sheet & Income Statement)
# ============================================================================

def parse_year_from_period(period_label: str) -> str:
    """
    ✅ FINANCIAL STATEMENTS ONLY
    
    Parse year from period label like '30-Jun-22', '30-Jun-2021', etc.
    Returns 4-digit year as string.
    Used for financial statement period validation.
    """
    if not period_label:
        return "Unknown"
    
    # Prefer 4-digit year; fallback to 2-digit (assume 20xx)
    m4 = re.search(r"\b(20\d{2}|19\d{2})\b", period_label)
    if m4:
        return m4.group(1)
    
    # Look for 2-digit year at the end (assume 20xx)
    # This should match patterns like "30-Jun-22", "Dec-21", etc.
    m2 = re.search(r"(\d{2})$", period_label)
    if m2:
        yy = int(m2.group(1))
        # Handle edge cases: 00-99 should be 2000-2099
        if yy < 50:  # Assume 20xx for years 00-49
            return f"20{yy:02d}"
        else:  # Assume 19xx for years 50-99
            return f"19{yy:02d}"
    
    return "Unknown"


def validate_document_years(doc: Dict[str, Any]) -> Tuple[bool, str]:
    """
    ✅ FINANCIAL STATEMENTS ONLY
    
    Validate that a document has exactly two years that differ by one year.
    Ensures consecutive year reporting for Balance Sheet & Income Statement.
    
    Returns:
        Tuple[bool, str]: (is_valid, reason)
    """
    period_current = doc.get("period_current", "")
    period_prior = doc.get("period_prior", "")
    
    if not period_current or not period_prior:
        return False, "Missing period information"
    
    year_current = parse_year_from_period(period_current)
    year_prior = parse_year_from_period(period_prior)
    
    if year_current == "Unknown" or year_prior == "Unknown":
        return False, f"Could not parse years from periods: {period_current}, {period_prior}"
    
    try:
        year_current_int = int(year_current)
        year_prior_int = int(year_prior)
        
        # Check if they differ by exactly 1 year
        year_diff = abs(year_current_int - year_prior_int)
        if year_diff != 1:
            return False, f"Years don't differ by 1: {year_current} vs {year_prior} (diff: {year_diff})"
        
        return True, f"Valid: {year_current} vs {year_prior}"
        
    except ValueError:
        return False, f"Invalid year format: {year_current}, {year_prior}"


def accumulate_statement_table(
    docs: List[Tuple[str, Dict[str, Any]]]
) -> Tuple[pd.DataFrame, List[str]]:
    """
    ✅ FINANCIAL STATEMENTS ONLY
    
    Aggregates Balance Sheet and Income Statement data across multiple files.
    Builds a mapping: label -> {year: value}
    Creates DataFrame with years as columns and financial items as rows.
    """
    # docs: list of (source_name, statement_dict)
    # Build mapping: label -> {year: value}
    label_to_year_to_value: Dict[str, Dict[str, float]] = {}
    years: Set[str] = set()

    for source_name, st_dict in docs:
        period_current = st_dict.get("period_current")
        period_prior = st_dict.get("period_prior")
        year_current = parse_year_from_period(period_current) if period_current else None
        year_prior = parse_year_from_period(period_prior) if period_prior else None

        for item in st_dict.get("items", []):
            label = item.get("label")
            if not label:
                continue
            year_to_value = label_to_year_to_value.setdefault(label, {})
            if year_current is not None:
                years.add(year_current)
                val_c = item.get("amount_current")
                if isinstance(val_c, (int, float)):
                    year_to_value[year_current] = float(val_c)
            if year_prior is not None:
                years.add(year_prior)
                val_p = item.get("amount_prior")
                if isinstance(val_p, (int, float)):
                    year_to_value[year_prior] = float(val_p)

    # Create DataFrame with all years as columns
    columns_sorted = sorted(list(years))
    df = pd.DataFrame.from_dict(label_to_year_to_value, orient="index")[columns_sorted]
    df.index.name = "Variable"
    
    # Fix Arrow serialization issues by ensuring consistent data types
    for col in df.columns:
        # Fill NaN with 0 and ensure all values are floats
        df[col] = df[col].fillna(0.0).astype(float)
    
    return df, columns_sorted


# ============================================================================
# CIC DOCUMENTS FUNCTIONS (Credit Information Center)
# ============================================================================

def process_cic_zip(
    file_bytes: bytes,
    tmp_root: str,
    progress_cb: Optional[Callable[[str, int, int], None]] = None,
    log_container: Optional[Any] = None,
) -> Tuple[List[Tuple[str, Dict[str, Any]]], List[str]]:
    """
    ✅ CIC DOCUMENTS ONLY
    
    Process a ZIP file containing CIC documents and extract credit account information.
    Uses pdfplumber (not OCR) to extract structured Field/Data tables.
    
    Returns:
        Tuple[List[Tuple[str, Dict[str, Any]]], List[str]]: (cic_docs, errors)
    """
    def log_and_display(message: str, level: str = "info"):
        if level == "info":
            logger.info(message)
        elif level == "error":
            logger.error(message)
        elif level == "warning":
            logger.warning(message)
        
        if log_container:
            with log_container.container():
                if level == "error":
                    st.error(f"🔴 {message}")
                elif level == "warning":
                    st.warning(f"🟡 {message}")
                else:
                    st.info(f"🔵 {message}")

    errors: List[str] = []
    cic_docs: List[Tuple[str, Dict[str, Any]]] = []
    
    zip_start = time.time()
    log_and_display("🏦 Starting CIC ZIP file processing...")
    
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        extract_start = time.time()
        extract_dir = tempfile.mkdtemp(prefix="cic_uploads_", dir=tmp_root)
        zf.extractall(extract_dir)
        log_and_display(f"✅ CIC ZIP extracted in {time.time() - extract_start:.2f}s")
        
        # Find PDFs
        search_start = time.time()
        pdf_paths: List[str] = []
        for root, _, files in os.walk(extract_dir):
            for f in files:
                if f.lower().endswith(".pdf"):
                    pdf_paths.append(os.path.join(root, f))
        
        log_and_display(f"🔍 Found {len(pdf_paths)} PDF files in CIC ZIP")
        
        if not pdf_paths:
            error_msg = "No PDF files found in CIC ZIP"
            errors.append(error_msg)
            log_and_display(error_msg, "error")
            return cic_docs, errors

        # Process each PDF
        for idx, pdf_path in enumerate(pdf_paths, start=1):
            file_start = time.time()
            source_name = os.path.basename(pdf_path)
            
            try:
                log_and_display(f"🏦 Processing CIC file {idx}/{len(pdf_paths)}: {source_name}")
                
                if progress_cb is not None:
                    progress_cb(pdf_path, idx, len(pdf_paths))
                
                # Extract CIC data
                cic_start = time.time()
                log_and_display(f"  🔍 Step 1/2: Extracting CIC data from {source_name}...")
                result = extract_cic_from_pdf(pdf_path)
                cic_time = time.time() - cic_start
                log_and_display(f"  ✅ CIC extraction complete in {cic_time:.2f}s")
                
                if result.get("success"):
                    total_accounts = result.get("total_accounts", 0)
                    start_page = result.get("start_page", 0)
                    end_page = result.get("end_page", 0)
                    pages_processed = end_page - start_page + 1
                    
                    log_and_display(f"  📊 CIC Results: {total_accounts} accounts from pages {start_page}-{end_page} ({pages_processed} pages)")
                    
                    cic_docs.append((source_name, result))
                else:
                    error_msg = f"CIC extraction failed: {result.get('error', 'Unknown error')}"
                    errors.append(f"{source_name}: {error_msg}")
                    log_and_display(f"  ❌ {error_msg}", "error")
                
                total_file_time = time.time() - file_start
                log_and_display(f"  🎉 {source_name} completed in {total_file_time:.2f}s")
                
            except Exception as e:
                error_msg = f"{source_name}: {str(e)}"
                errors.append(error_msg)
                log_and_display(f"  ❌ Failed to process {source_name}: {str(e)}", "error")
    
    total_zip_time = time.time() - zip_start
    log_and_display(f"🏁 CIC ZIP processing complete in {total_zip_time:.2f}s")
    log_and_display(f"📊 CIC Summary: {len(cic_docs)} successful, {len(errors)} failed")
    
    return cic_docs, errors

# ============================================================================
# FINANCIAL STATEMENTS PROCESSING FUNCTION
# ============================================================================

def process_zip(
    file_bytes: bytes,
    tmp_root: str,
    progress_cb: Optional[Callable[[str, int, int], None]] = None,
    log_container: Optional[Any] = None,
) -> Tuple[List[Tuple[str, Dict[str, Any]]], List[Tuple[str, Dict[str, Any]]], List[str]]:
    """
    ✅ FINANCIAL STATEMENTS ONLY
    
    Process ZIP files containing Financial Statement PDFs.
    Uses OCR (Tesseract) for scanned PDFs, then extracts Balance Sheet & Income Statement.
    
    Returns:
        Tuple[List[Tuple[str, Dict[str, Any]]], List[Tuple[str, Dict[str, Any]]], List[str]]:
        (balance_docs, income_docs, errors)
    """
    def log_and_display(message: str, level: str = "info"):
        if level == "info":
            logger.info(message)
        elif level == "error":
            logger.error(message)
        elif level == "warning":
            logger.warning(message)
        
        if log_container:
            with log_container.container():
                if level == "error":
                    st.error(f"🔴 {message}")
                elif level == "warning":
                    st.warning(f"🟡 {message}")
                else:
                    st.info(f"🔵 {message}")

    errors: List[str] = []
    balance_docs: List[Tuple[str, Dict[str, Any]]] = []
    income_docs: List[Tuple[str, Dict[str, Any]]] = []
    
    zip_start = time.time()
    log_and_display("📦 Starting ZIP file processing...")
    
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        extract_start = time.time()
        extract_dir = tempfile.mkdtemp(prefix="uploads_", dir=tmp_root)
        zf.extractall(extract_dir)
        log_and_display(f"✅ ZIP extracted in {time.time() - extract_start:.2f}s")
        
        # Find PDFs
        search_start = time.time()
        pdf_paths: List[str] = []
        for root, _, files in os.walk(extract_dir):
            for f in files:
                if f.lower().endswith(".pdf"):
                    pdf_paths.append(os.path.join(root, f))
        
        log_and_display(f"🔍 Found {len(pdf_paths)} PDF files in {time.time() - search_start:.2f}s")
        
        if not pdf_paths:
            error_msg = "No PDF files found in ZIP"
            errors.append(error_msg)
            log_and_display(error_msg, "error")
            return balance_docs, income_docs, errors

        # Process each PDF
        for idx, pdf_path in enumerate(pdf_paths, start=1):
            file_start = time.time()
            source_name = os.path.basename(pdf_path)
            
            try:
                log_and_display(f"📄 Processing file {idx}/{len(pdf_paths)}: {source_name}")
                
                if progress_cb is not None:
                    progress_cb(pdf_path, idx, len(pdf_paths))
                
                # Step 1: OCR
                ocr_start = time.time()
                log_and_display(f"  🔍 Step 1/3: Running OCR on {source_name}...")
                entries = ocr_pdf_to_lines(pdf_path, dpi=300, psm=6, conf_threshold=10)
                log_and_display(f"  ✅ OCR complete: {len(entries)} lines in {time.time() - ocr_start:.2f}s")
                
                # Step 2: Extraction
                extract_start = time.time()
                log_and_display(f"  📊 Step 2/3: Extracting statements from {source_name}...")
                balance, income = extract_statements_from_entries(entries)
                log_and_display(f"  ✅ Extraction complete in {time.time() - extract_start:.2f}s")
                
                # Step 3: Results - Conditionally add only if statements have items
                bs_items = len(balance.get('items', []))
                is_items = len(income.get('items', []))
                bs_periods = f"{balance.get('period_current', 'N/A')} vs {balance.get('period_prior', 'N/A')}"
                is_periods = f"{income.get('period_current', 'N/A')} vs {income.get('period_prior', 'N/A')}"
                
                # Log results for each statement that exists
                if bs_items > 0:
                    log_and_display(f"  📈 Balance Sheet: {bs_items} items ({bs_periods})")
                    balance_docs.append((source_name, balance))
                else:
                    log_and_display(f"  ⚠️ Balance Sheet: Not found or empty", "warning")
                
                if is_items > 0:
                    log_and_display(f"  📉 Income Statement: {is_items} items ({is_periods})")
                    income_docs.append((source_name, income))
                else:
                    log_and_display(f"  ⚠️ Income Statement: Not found or empty", "warning")
                
                # If neither statement was found, log a warning but don't fail
                if bs_items == 0 and is_items == 0:
                    log_and_display(f"  ⚠️ No financial statements found in {source_name}", "warning")
                
                total_file_time = time.time() - file_start
                log_and_display(f"  🎉 {source_name} completed in {total_file_time:.2f}s")
                
            except Exception as e:
                error_msg = f"{source_name}: {str(e)}"
                errors.append(error_msg)
                log_and_display(f"  ❌ Failed to process {source_name}: {str(e)}", "error")
    
    total_zip_time = time.time() - zip_start
    log_and_display(f"🏁 ZIP processing complete in {total_zip_time:.2f}s")
    log_and_display(f"📊 Summary: {len(balance_docs)} successful, {len(errors)} failed")
    
    return balance_docs, income_docs, errors


st.set_page_config(page_title="Financial Document Processor", layout="wide")
st.title("Financial Document Processor")
st.caption("Process ZIP files containing Financial Statements and/or CIC (Credit Information) documents.")

# ============================================================================
# STREAMLIT UI SETUP
# ============================================================================

# Create tabs for different document types
tab1, tab2 = st.tabs(["📊 Financial Statements", "🏦 CIC Documents"])

# ============================================================================
# TAB 1: FINANCIAL STATEMENTS (Balance Sheet & Income Statement)
# ============================================================================
with tab1:
    st.subheader("📊 Financial Statements: Balance Sheet and Income Statement")
    st.caption("Upload ZIP files containing PDFs. Extracted tables aggregate by year across all files.")
    
    uploaded_zips = st.file_uploader(
        "Upload ZIP archives containing Financial Statement PDFs",
        type=["zip"],
        accept_multiple_files=True,
        key="financial_zips"
    )
    process_fs = st.button("Process Financial Statements", key="process_fs_button")

if uploaded_zips and process_fs:
    # Create containers for different parts of the UI
    timer_ph = st.empty()
    progress_container = st.container()
    log_container = st.expander("📋 Processing Logs", expanded=True)
    
    start_time = time.perf_counter()
    def update_timer(prefix: str = "") -> None:
        elapsed = int(time.perf_counter() - start_time)
        mm = elapsed // 60
        ss = elapsed % 60
        timer_ph.markdown(f"**⏱️ {mm:02d}:{ss:02d}** {prefix}")

    with st.spinner("Processing ZIP files..."):
        logger.info(f"🚀 Starting Streamlit processing session with {len(uploaded_zips)} ZIP files")
        
        tmp_root = tempfile.mkdtemp(prefix="st_runs_")
        all_balance_docs: List[Tuple[str, Dict[str, Any]]] = []
        all_income_docs: List[Tuple[str, Dict[str, Any]]] = []
        all_errors: List[str] = []

        for zip_idx, up in enumerate(uploaded_zips, 1):
            zip_start = time.time()
            try:
                with log_container.container():
                    st.markdown(f"### 📦 Processing ZIP {zip_idx}/{len(uploaded_zips)}: {up.name}")
                
                logger.info(f"📦 Starting ZIP {zip_idx}/{len(uploaded_zips)}: {up.name} ({up.size} bytes)")
                
                def progress_cb(pdf_path: str, idx: int, total: int) -> None:
                    pdf_name = os.path.basename(pdf_path)
                    update_timer(prefix=f"ZIP {zip_idx}: File {idx}/{total} - {pdf_name}")
                
                bdocs, idocs, errs = process_zip(
                    up.getbuffer(), 
                    tmp_root, 
                    progress_cb=progress_cb,
                    log_container=log_container
                )
                
                all_balance_docs.extend(bdocs)
                all_income_docs.extend(idocs)
                all_errors.extend(errs)
                
                zip_time = time.time() - zip_start
                success_msg = f"✅ {up.name}: {len(bdocs)} files processed in {zip_time:.2f}s"
                logger.info(success_msg)
                
                with log_container.container():
                    st.success(success_msg)
                
            except Exception as e:
                error_msg = f"{up.name}: {e}"
                all_errors.append(error_msg)
                logger.error(f"❌ ZIP processing failed: {error_msg}")
                
                with log_container.container():
                    st.error(f"❌ {error_msg}")

        total_time = time.perf_counter() - start_time
        update_timer(prefix="✅ Processing complete!")
        
        logger.info(f"🏁 Session complete: {len(all_balance_docs)} successful files, {len(all_errors)} errors in {total_time:.2f}s")
        
        with log_container.container():
            st.markdown("---")
            st.markdown(f"### 📊 Final Summary")
            st.markdown(f"- **Total files processed:** {len(all_balance_docs)}")
            st.markdown(f"- **Total errors:** {len(all_errors)}")
            st.markdown(f"- **Total processing time:** {total_time:.2f}s")

        if all_errors:
            st.warning("⚠️ Some files had errors:")
            for error in all_errors:
                st.error(f"• {error}")

        # Display Balance Sheet only if it exists
        if all_balance_docs:
            st.subheader("Balance Sheet (aggregated by year)")
            df_bs, cols_bs = accumulate_statement_table(all_balance_docs)
            # Format display for better readability (show 0 instead of 0.0)
            df_bs_display = df_bs.copy()
            for col in df_bs_display.columns:
                df_bs_display[col] = df_bs_display[col].apply(lambda x: "" if x == 0.0 else f"{x:,.0f}" if x == int(x) else f"{x:,.2f}")
            st.dataframe(df_bs_display)
            csv_bs = df_bs.to_csv().encode("utf-8")
            st.download_button("Download Balance Sheet CSV", data=csv_bs, file_name="balance_sheet_aggregated.csv")
        else:
            st.info("ℹ️ No Balance Sheet data found in the processed files.")

        # Display Income Statement only if it exists
        if all_income_docs:
            st.subheader("Income Statement (aggregated by year)")
            df_is, cols_is = accumulate_statement_table(all_income_docs)
            # Format display for better readability (show 0 instead of 0.0)
            df_is_display = df_is.copy()
            for col in df_is_display.columns:
                df_is_display[col] = df_is_display[col].apply(lambda x: "" if x == 0.0 else f"{x:,.0f}" if x == int(x) else f"{x:,.2f}")
            st.dataframe(df_is_display)
            csv_is = df_is.to_csv().encode("utf-8")
            st.download_button("Download Income Statement CSV", data=csv_is, file_name="income_statement_aggregated.csv")
        else:
            st.info("ℹ️ No Income Statement data found in the processed files.")

        # CoopBank SME — Financial Position (Financial 40 sheet, Manufacturing profile)
        bs_by_n = dict(all_balance_docs)
        is_by_n = dict(all_income_docs)
        common_names = sorted(set(bs_by_n.keys()) & set(is_by_n.keys()))
        if common_names:
            st.markdown("---")
            st.subheader("🏛️ CoopBank SME grading — Financial & industry (spreadsheet-aligned)")
            st.caption(
                "Rubric_source: *CoopBank Risk grading for SMEs.xlsx* — *Financial 40* and *Industry attractiveness 10%*. "
                "Pick the sector column to score (Manufacturing/Agriculture). This does not replace manual credit judgement."
            )
            pick = st.selectbox("Source PDF for ratio analysis (must have both BS and IS)", common_names)
            sector = st.selectbox(
                "Sector column (Financial 40 thresholds)",
                [
                    ("manufacturing", "Manufacturing"),
                    ("agriculture", "Agriculture"),
                    ("import_dts", "Import & DTS"),
                    ("export", "Export"),
                    ("construction_machinery", "Construction & Machinery"),
                ],
                format_func=lambda x: x[1],
                key="coopbank_sector",
            )[0]
            fsq = st.selectbox(
                "Quality of financial statements (manual)",
                [
                    ("all_audited_up_to_date", "All audited, up to date"),
                    ("audited_one_provisional", "Audited + one year provisional"),
                    ("audited_two_provisional", "Audited + two years provisional"),
                    ("all_provisional", "All provisional"),
                    ("commercial_credit_report", "Commercial credit report"),
                ],
                format_func=lambda x: x[1],
                key="coopbank_fsq",
            )[0]
            neg_adj = st.selectbox(
                "Negative values / discrepancy adjustment",
                [
                    ("none", "None"),
                    ("minor", "Minor discrepancy (−0.02)"),
                    ("major", "Major discrepancy (−0.05)"),
                ],
                format_func=lambda x: x[1],
                key="coopbank_neg",
            )[0]
            st.markdown("**Industry attractiveness (10%) — manual inputs**")
            ic1, ic2, ic3 = st.columns(3)
            with ic1:
                outlook = st.selectbox(
                    "Business outlook",
                    ["stable", "favorable", "unstable"],
                    key="coopbank_outlook",
                )
            with ic2:
                comp = st.selectbox(
                    "Market competition",
                    ["acceptable", "dominant player", "weak player"],
                    key="coopbank_comp",
                )
            with ic3:
                org = st.selectbox(
                    "Form of organization",
                    ["private limited company", "cooperatives", "share company", "sole proprietorship"],
                    key="coopbank_org",
                )
            fin = compute_financial_position_40(
                bs_by_n[pick],
                is_by_n[pick],
                sector=sector,
                financial_statement_quality=fsq,
                negative_values_adjustment=neg_adj,
            )
            ind = industry_attractiveness_10(outlook, comp, org)

            st.markdown("**Management capacity (9%) — manual inputs**")
            mc1, mc2, mc3 = st.columns(3)
            with mc1:
                exp_years = st.number_input("Experience in related line (years)", min_value=0.0, value=3.0, step=0.5, key="coopbank_mgmt_exp")
            with mc2:
                qual = st.selectbox(
                    "Qualification (top management)",
                    [
                        ("degree_and_above", "51%+ are 1st degree and above graduates"),
                        ("diploma_and_above", "51%+ are diploma and above graduates"),
                        ("high_school_and_above", "51%+ are high school and above graduates"),
                        ("below_high_school", "51%+ are below high school"),
                    ],
                    format_func=lambda x: x[1],
                    key="coopbank_mgmt_qual",
                )[0]
            with mc3:
                duty = st.selectbox(
                    "Duty segregation",
                    [
                        ("clear", "Clear outlined duties & org structure"),
                        ("semi", "Semi-segregated duties & org structure"),
                        ("unclear", "Unclear duties / control breached"),
                    ],
                    format_func=lambda x: x[1],
                    key="coopbank_mgmt_duty",
                )[0]
            mgmt = management_capacity_9(experience_years=exp_years, qualification=qual, duty_segregation=duty)

            st.markdown("**Integrity (6%) — manual inputs**")
            in1, in2, in3 = st.columns(3)
            with in1:
                cons = st.selectbox(
                    "Consistency of presented documents",
                    [("adequate", "Adequate"), ("acceptable", "Acceptable"), ("inconsistent", "Inconsistent")],
                    format_func=lambda x: x[1],
                    key="coopbank_int_cons",
                )[0]
            with in2:
                resp = st.selectbox(
                    "Responsiveness to bank inquiry",
                    [
                        ("provides_consistently_prompt", "Provides consistently prompt"),
                        ("response_and_willingly", "Responds and willingly"),
                        ("responds_with_repeated_inquiry", "Responds with repeated inquiry"),
                        ("not_responsive_or_willing", "Not responsive or willing"),
                    ],
                    format_func=lambda x: x[1],
                    key="coopbank_int_resp",
                )[0]
            with in3:
                tax = st.selectbox(
                    "Corporate responsibility (Tax payment)",
                    [("perfect", "Perfect"), ("acceptable", "Acceptable"), ("inconsistent", "Inconsistent")],
                    format_func=lambda x: x[1],
                    key="coopbank_int_tax",
                )[0]
            integ = integrity_6(consistency=cons, responsiveness=resp, tax_payment=tax)

            st.markdown("**Banking relationship (10%) — manual inputs**")
            br1, br2 = st.columns(2)
            with br1:
                rel_years = st.number_input("Length of relationship with CBO (years)", min_value=0.0, value=1.0, step=0.5, key="coopbank_bank_years")
            with br2:
                share_pct = st.number_input("Account transaction share to sales (%)", min_value=0.0, value=70.0, step=1.0, key="coopbank_bank_share")
            bank = banking_relationship_10(relationship_years=rel_years, transaction_share_pct=share_pct)

            subtotal = (
                fin["financial_position_decimal"]
                + ind["industry_decimal"]
                + mgmt["management_decimal"]
                + integ["integrity_decimal"]
                + bank["banking_decimal"]
            )
            subtotal_max = 0.40 + 0.10 + 0.09 + 0.06 + 0.10
            ach = (subtotal / subtotal_max) * 100.0 if subtotal_max else 0.0
            grade = interpret_business_risk_grade(ach)
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Financial position (max 0.40)", f"{fin['financial_position_decimal']:.3f}")
            m2.metric("Industry (max 0.10)", f"{ind['industry_decimal']:.3f}")
            m3.metric("Subtotal achievement %", f"{ach:.1f}%")
            m4.metric("Indicative grade", f"Grade {grade['grade']}")
            st.caption(
                f"{grade['risk_level']} — {grade['bankability']} "
                f"(Bands from *Business Risk Summary* sheet, applied to this **subtotal** block.)"
            )
            with st.expander("CoopBank subtotal detail (inputs & bands)", expanded=False):
                st.json(fin)
                st.json(ind)
                st.json(mgmt)
                st.json(integ)
                st.json(bank)

        # Per-file tables
        st.subheader("Per-file tables")
        include_invalid = st.checkbox("Include files without consecutive years", value=False)
        
        # Build lookup by source name and validate each document
        bs_by_file: Dict[str, Dict[str, Any]] = {}
        is_by_file: Dict[str, Dict[str, Any]] = {}
        
        # Track validation results
        validation_results: Dict[str, Dict[str, Tuple[bool, str]]] = {}
        
        for name, doc in all_balance_docs:
            is_valid, reason = validate_document_years(doc)
            if name not in validation_results:
                validation_results[name] = {}
            validation_results[name]['balance_sheet'] = (is_valid, reason)
            if is_valid:
                bs_by_file[name] = doc
        
        for name, doc in all_income_docs:
            is_valid, reason = validate_document_years(doc)
            if name not in validation_results:
                validation_results[name] = {}
            validation_results[name]['income_statement'] = (is_valid, reason)
            if is_valid:
                is_by_file[name] = doc
        
        # Get all file names (both valid and invalid for reporting)
        all_file_names = sorted(set(list(validation_results.keys())))
        valid_file_names = sorted(set(list(bs_by_file.keys()) + list(is_by_file.keys())))
        # Build lookup for all extracted docs regardless of validation
        bs_all_by_file: Dict[str, Dict[str, Any]] = {name: doc for name, doc in all_balance_docs}
        is_all_by_file: Dict[str, Dict[str, Any]] = {name: doc for name, doc in all_income_docs}
        
        # Show validation summary
        if len(valid_file_names) < len(all_file_names):
            invalid_count = len(all_file_names) - len(valid_file_names)
            st.warning(f"⚠️ {invalid_count} files excluded (don't have exactly 2 consecutive years)")
            
            with st.expander("📋 Validation Details", expanded=False):
                for fname in all_file_names:
                    if fname not in valid_file_names:
                        st.error(f"❌ **{fname}**")
                        if 'balance_sheet' in validation_results[fname]:
                            bs_valid, bs_reason = validation_results[fname]['balance_sheet']
                            if not bs_valid:
                                st.text(f"   Balance Sheet: {bs_reason}")
                        if 'income_statement' in validation_results[fname]:
                            is_valid, is_reason = validation_results[fname]['income_statement']
                            if not is_valid:
                                st.text(f"   Income Statement: {is_reason}")
        
        # Display documents
        names_to_show = valid_file_names if not include_invalid else all_file_names
        if names_to_show:
            if include_invalid:
                st.info(f"ℹ️ Showing {len(names_to_show)} documents (including non-consecutive years)")
            else:
                st.success(f"✅ Showing {len(names_to_show)} valid documents with 2 consecutive years")
            
            for fname in names_to_show:
                st.markdown(f"### 📄 {fname}")
                
                # Show the validation info for this file
                validation_info = []
                if fname in validation_results and 'balance_sheet' in validation_results[fname]:
                    _, bs_reason = validation_results[fname]['balance_sheet']
                    if bs_reason.startswith("Valid:"):
                        validation_info.append(f"BS: {bs_reason[7:]}")  # Remove "Valid: " prefix
                if fname in validation_results and 'income_statement' in validation_results[fname]:
                    _, is_reason = validation_results[fname]['income_statement']
                    if is_reason.startswith("Valid:"):
                        validation_info.append(f"IS: {is_reason[7:]}")  # Remove "Valid: " prefix
                
                if validation_info:
                    st.caption(f"📅 Years: {' | '.join(validation_info)}")
                
                # Determine which statements exist for this file
                bs_source = bs_by_file if not include_invalid else bs_all_by_file
                is_source = is_by_file if not include_invalid else is_all_by_file
                has_bs = fname in bs_source and len(bs_source[fname].get('items', [])) > 0
                has_is = fname in is_source and len(is_source[fname].get('items', [])) > 0
                
                # Skip file if no statements found
                if not has_bs and not has_is:
                    st.warning("⚠️ No valid financial statements found for this file.")
                    continue
                
                # Display statements based on what exists
                if has_bs and has_is:
                    # Both exist - show side by side
                    cols = st.columns(2)
                    
                    # Balance Sheet
                    df_file_bs, _ = accumulate_statement_table([(fname, bs_source[fname])])
                    df_file_bs_display = df_file_bs.copy()
                    for col in df_file_bs_display.columns:
                        df_file_bs_display[col] = df_file_bs_display[col].apply(lambda x: "" if x == 0.0 else f"{x:,.0f}" if x == int(x) else f"{x:,.2f}")
                    cols[0].markdown("**Balance Sheet**")
                    cols[0].dataframe(df_file_bs_display, use_container_width=True)
                    
                    # Income Statement
                    df_file_is, _ = accumulate_statement_table([(fname, is_source[fname])])
                    df_file_is_display = df_file_is.copy()
                    for col in df_file_is_display.columns:
                        df_file_is_display[col] = df_file_is_display[col].apply(lambda x: "" if x == 0.0 else f"{x:,.0f}" if x == int(x) else f"{x:,.2f}")
                    cols[1].markdown("**Income Statement**")
                    cols[1].dataframe(df_file_is_display, use_container_width=True)
                    
                elif has_bs:
                    # Only Balance Sheet exists
                    st.markdown("**Balance Sheet**")
                    df_file_bs, _ = accumulate_statement_table([(fname, bs_source[fname])])
                    df_file_bs_display = df_file_bs.copy()
                    for col in df_file_bs_display.columns:
                        df_file_bs_display[col] = df_file_bs_display[col].apply(lambda x: "" if x == 0.0 else f"{x:,.0f}" if x == int(x) else f"{x:,.2f}")
                    st.dataframe(df_file_bs_display, use_container_width=True)
                    st.info("ℹ️ Income Statement not found for this file.")
                    
                elif has_is:
                    # Only Income Statement exists
                    st.markdown("**Income Statement**")
                    df_file_is, _ = accumulate_statement_table([(fname, is_source[fname])])
                    df_file_is_display = df_file_is.copy()
                    for col in df_file_is_display.columns:
                        df_file_is_display[col] = df_file_is_display[col].apply(lambda x: "" if x == 0.0 else f"{x:,.0f}" if x == int(x) else f"{x:,.2f}")
                    st.dataframe(df_file_is_display, use_container_width=True)
                    st.info("ℹ️ Balance Sheet not found for this file.")
        else:
            if include_invalid:
                st.error("❌ No documents available to display")
            else:
                st.error("❌ No documents have exactly 2 consecutive years")

    elapsed = int(time.perf_counter() - start_time)
    st.success(f"Done in {elapsed//60:02d}:{elapsed%60:02d}")
else:
    st.info("Awaiting Financial Statement ZIP uploads...")

# ============================================================================
# TAB 2: CIC DOCUMENTS (Credit Information Center)
# ============================================================================
with tab2:
    st.subheader("🏦 CIC Documents: Credit Account Information")
    st.caption("Upload ZIP files containing CIC (Credit Information) PDFs. Each account becomes a row in the table.")
    
    uploaded_cic_zips = st.file_uploader(
        "Upload ZIP archives containing CIC PDFs",
        type=["zip"],
        accept_multiple_files=True,
        key="cic_zips"
    )
    process_cic = st.button("Process CIC Documents", key="process_cic_button")
    
    if uploaded_cic_zips and process_cic:
        # Create containers for different parts of the UI
        cic_timer_ph = st.empty()
        cic_progress_container = st.container()
        cic_log_container = st.expander("📋 CIC Processing Logs", expanded=True)
        
        cic_start_time = time.perf_counter()
        def update_cic_timer(prefix: str = "") -> None:
            elapsed = int(time.perf_counter() - cic_start_time)
            mm = elapsed // 60
            ss = elapsed % 60
            cic_timer_ph.markdown(f"**⏱️ {mm:02d}:{ss:02d}** {prefix}")

        with st.spinner("Processing CIC ZIP files..."):
            logger.info(f"🚀 Starting CIC Streamlit processing session with {len(uploaded_cic_zips)} ZIP files")
            
            tmp_root = tempfile.mkdtemp(prefix="cic_st_runs_")
            all_cic_docs: List[Tuple[str, Dict[str, Any]]] = []
            all_cic_errors: List[str] = []

            for zip_idx, up in enumerate(uploaded_cic_zips, 1):
                cic_zip_start = time.time()
                try:
                    with cic_log_container.container():
                        st.markdown(f"### 📦 Processing CIC ZIP {zip_idx}/{len(uploaded_cic_zips)}: {up.name}")
                    
                    logger.info(f"📦 Starting CIC ZIP {zip_idx}/{len(uploaded_cic_zips)}: {up.name} ({up.size} bytes)")
                    
                    def cic_progress_cb(pdf_path: str, idx: int, total: int) -> None:
                        pdf_name = os.path.basename(pdf_path)
                        update_cic_timer(prefix=f"CIC ZIP {zip_idx}: File {idx}/{total} - {pdf_name}")
                    
                    cic_docs, cic_errs = process_cic_zip(
                        up.getbuffer(), 
                        tmp_root, 
                        progress_cb=cic_progress_cb,
                        log_container=cic_log_container
                    )
                    
                    all_cic_docs.extend(cic_docs)
                    all_cic_errors.extend(cic_errs)
                    
                    cic_zip_time = time.time() - cic_zip_start
                    success_msg = f"✅ {up.name}: {len(cic_docs)} files processed in {cic_zip_time:.2f}s"
                    logger.info(success_msg)
                    
                    with cic_log_container.container():
                        st.success(success_msg)
                    
                except Exception as e:
                    error_msg = f"{up.name}: {e}"
                    all_cic_errors.append(error_msg)
                    logger.error(f"❌ CIC ZIP processing failed: {error_msg}")
                    
                    with cic_log_container.container():
                        st.error(f"❌ {error_msg}")

            total_cic_time = time.perf_counter() - cic_start_time
            update_cic_timer(prefix="✅ CIC Processing complete!")
            
            logger.info(f"🏁 CIC Session complete: {len(all_cic_docs)} successful files, {len(all_cic_errors)} errors in {total_cic_time:.2f}s")
            
            with cic_log_container.container():
                st.markdown("---")
                st.markdown(f"### 📊 CIC Final Summary")
                st.markdown(f"- **Total files processed:** {len(all_cic_docs)}")
                st.markdown(f"- **Total errors:** {len(all_cic_errors)}")
                st.markdown(f"- **Total processing time:** {total_cic_time:.2f}s")

            if all_cic_errors:
                st.warning("⚠️ Some CIC files had errors:")
                for error in all_cic_errors:
                    st.error(f"• {error}")

            if all_cic_docs:
                st.subheader("🏦 Credit Rating Analysis Dashboard")
                st.markdown("### 📊 Expert Credit Risk Assessment & Classification")
                
                # Aggregate all accounts for overall analysis
                all_credit_accounts = []
                for filename, cic_data in all_cic_docs:
                    if cic_data.get("success"):
                        all_credit_accounts.extend(cic_data.get("credit_accounts", []))
                
                if all_credit_accounts:
                    # Perform comprehensive credit rating analysis
                    overall_analysis = analyze_credit_accounts(all_credit_accounts)
                    
                    # Display Overall Credit Exposure Summary
                    st.markdown("---")
                    st.markdown("### 💰 Aggregate Credit Exposure")
                    exposure = overall_analysis["credit_exposure"]
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Approved Amount", f"${exposure['total_approved_amount']:,.2f}")
                    with col2:
                        st.metric("Total Current Balance", f"${exposure['total_current_balance']:,.2f}")
                    with col3:
                        st.metric("Active Accounts", exposure['active_accounts_count'])
                    with col4:
                        st.metric("Settled Accounts", exposure['settled_accounts_count'])
                    
                    # Repayment Status Breakdown
                    st.markdown("---")
                    st.markdown("### 📈 Existing Loan Repayment Status Breakdown")
                    repayment_breakdown = overall_analysis["repayment_status_breakdown"]
                    if repayment_breakdown:
                        repayment_df = pd.DataFrame([
                            {"Status": status, "Count": count}
                            for status, count in sorted(repayment_breakdown.items(), key=lambda x: x[1], reverse=True)
                        ])
                        st.dataframe(repayment_df, use_container_width=True, hide_index=True)
                    else:
                        st.info("No active loan repayment data available")
                    
                    # Settled Loans Breakdown
                    st.markdown("---")
                    st.markdown("### ✅ Settled Loans Breakdown")
                    settled_breakdown = overall_analysis["settled_loans_breakdown"]
                    if settled_breakdown:
                        settled_df = pd.DataFrame([
                            {"Settlement Status": status, "Count": count}
                            for status, count in sorted(settled_breakdown.items(), key=lambda x: x[1], reverse=True)
                        ])
                        st.dataframe(settled_df, use_container_width=True, hide_index=True)
                    else:
                        st.info("No settled loan data available")
                    
                    # Credit Exposure by Risk Classification
                    if exposure.get("total_by_risk_classification"):
                        st.markdown("---")
                        st.markdown("### ⚠️ Credit Exposure by Risk Classification")
                        risk_df = pd.DataFrame([
                            {"Risk Classification": risk, "Total Amount": f"${amount:,.2f}"}
                            for risk, amount in sorted(exposure["total_by_risk_classification"].items(), key=lambda x: x[1], reverse=True)
                        ])
                        st.dataframe(risk_df, use_container_width=True, hide_index=True)
                    
                    # Credit Exposure by Institution
                    if exposure.get("total_by_institution"):
                        st.markdown("---")
                        st.markdown("### 🏦 Credit Exposure by Institution")
                        inst_df = pd.DataFrame([
                            {"Institution": inst, "Total Amount": f"${amount:,.2f}"}
                            for inst, amount in sorted(exposure["total_by_institution"].items(), key=lambda x: x[1], reverse=True)
                        ])
                        st.dataframe(inst_df, use_container_width=True, hide_index=True)

                    cb = overall_analysis.get("coopbank_grading") or {}
                    cic_auto = (cb.get("credit_history_cic_automated") or {})
                    if cic_auto:
                        st.markdown("---")
                        st.markdown("### 🏛️ CoopBank SME — Credit History (CIC-automated)")
                        st.caption(
                            "Weights from *Credit History 25%* sheet: **Term/loan performance** (existing + settled) "
                            "+ **Aggregate credit exposure**. "
                            "Term-loan portfolio score shows **mean** and **worst facility** (min); primary total uses the mean."
                        )
                        tl = cic_auto.get("term_loan_performance") or {}
                        ex = cic_auto.get("aggregate_credit_exposure") or {}
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Term loan (mean, decimal)", f"{tl.get('term_loan_mean_decimal', 0):.3f}")
                        c2.metric("Term loan (worst facility)", f"{tl.get('term_loan_worst_decimal', 0):.3f}")
                        c3.metric("Aggregate exposure score", f"{ex.get('decimal_score', 0):.3f}")
                        c4.metric("CIC block total (max 0.25)", f"{cic_auto.get('cic_automated_total_decimal', 0):.3f}")
                        st.caption(f"Exposure band: **{ex.get('band', '')}** — active approved total **${ex.get('total_active_approved_amount', 0):,.2f}**")
                        ach_cic = 0.0
                        if cic_auto.get("cic_automated_max_decimal"):
                            ach_cic = (
                                cic_auto["cic_automated_total_decimal"]
                                / cic_auto["cic_automated_max_decimal"]
                            ) * 100.0
                        g_cic = interpret_business_risk_grade(ach_cic)
                        st.info(
                            f"CIC-only achievement vs max automated band: **{ach_cic:.1f}%** → "
                            f"Grade **{g_cic['grade']}** ({g_cic['risk_level']}). "
                            "This is **not** a full obligor grade — full form includes overdraft, L/C, guarantees, management, etc."
                        )
                        bdf = tl.get("per_account") or []
                        if bdf:
                            st.dataframe(pd.DataFrame(bdf), use_container_width=True, hide_index=True)
                
                # Show separate detailed table for each file with enhanced classifications
                st.markdown("---")
                st.subheader("📋 Detailed Account Information by File")
                
                for filename, cic_data in all_cic_docs:
                    if not cic_data.get("success"):
                        continue
                        
                    st.markdown(f"### 📄 {filename}")
                    
                    credit_accounts = cic_data.get("credit_accounts", [])
                    if credit_accounts:
                        # Perform analysis for this file
                        file_analysis = analyze_credit_accounts(credit_accounts)
                        enhanced_accounts = file_analysis["enhanced_accounts"]
                        
                        # Create enhanced table with all fields plus new classifications
                        table_data = []
                        for account in enhanced_accounts:
                            row = {
                                "institution": account.get("institution", ""),
                                "account_status": account.get("account_status", ""),
                                "coopbank_bucket": account.get("coopbank_performance_bucket", ""),
                                "coopbank_term_loan_decimal": account.get("coopbank_term_loan_decimal", ""),
                                "repayment_status": account.get("repayment_status", account.get("settled_loan_status", "")),
                                "approved_amount": account.get("approved_amount", ""),
                                "current_balance": account.get("current_balance_amount", ""),
                                "date_account_opened": account.get("date_account_opened", ""),
                                "payment_due_date": account.get("payment_due_date", ""),
                                "credit_account_risk_classification": account.get("credit_account_risk_classification", ""),
                                "role_in_credit_account": account.get("role_in_credit_account", ""),
                                "product_type": account.get("product_type", ""),
                                "account_number": account.get("account_number", "")
                            }
                            table_data.append(row)
                        
                        df = pd.DataFrame(table_data)
                        if not df.empty:
                            # Sort by status, then institution
                            if 'account_status' in df.columns:
                                df = df.sort_values(['account_status', 'institution'])
                            
                            st.dataframe(df, use_container_width=True)
                            
                            # Download button for enhanced data
                            csv_data = df.to_csv(index=False).encode("utf-8")
                            st.download_button(
                                f"Download Enhanced {filename} CSV", 
                                data=csv_data, 
                                file_name=f"{filename.replace('.pdf', '')}_enhanced_cic_analysis.csv",
                                mime="text/csv"
                            )
                            
                            # Show file-level summary
                            file_exposure = file_analysis["credit_exposure"]
                            with st.expander(f"📊 {filename} - Credit Exposure Summary"):
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.write(f"**Total Approved:** ${file_exposure['total_approved_amount']:,.2f}")
                                    st.write(f"**Total Balance:** ${file_exposure['total_current_balance']:,.2f}")
                                with col2:
                                    st.write(f"**Active Accounts:** {file_exposure['active_accounts_count']}")
                                    st.write(f"**Settled Accounts:** {file_exposure['settled_accounts_count']}")
                                with col3:
                                    st.write(f"**Total Accounts:** {file_analysis['total_accounts']}")
                                fcb = file_analysis.get("coopbank_grading", {}).get("credit_history_cic_automated")
                                if fcb:
                                    st.markdown("**CoopBank CIC block (this file)**")
                                    st.write(
                                        f"Term loan (mean): **{fcb['term_loan_performance']['term_loan_mean_decimal']:.3f}** — "
                                        f"exposure: **{fcb['aggregate_credit_exposure']['decimal_score']:.3f}** "
                                        f"({fcb['aggregate_credit_exposure']['band']})"
                                    )
                    else:
                        st.info("No credit accounts found in this file")
                    
                    st.markdown("---")
            else:
                st.error("❌ No CIC documents were successfully processed")
                
        elapsed = int(time.perf_counter() - cic_start_time)
        st.success(f"CIC Processing completed in {elapsed//60:02d}:{elapsed%60:02d}")
    else:
        st.info("Awaiting CIC ZIP uploads...")



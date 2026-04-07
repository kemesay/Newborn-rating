"""
Enhanced FastAPI for Financial Document Processing
Supports both Financial Statements (Balance Sheet & Income Statement) and CIC Documents
with comprehensive credit rating analysis.
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import uvicorn
import tempfile
import os
import time
import logging
from datetime import datetime

# ============================================================================
# FINANCIAL STATEMENTS MODULES
# ============================================================================
from ocr_to_lines import ocr_pdf_to_lines
from extract_statements import extract_statements_from_entries

# ============================================================================
# CIC DOCUMENTS MODULES
# ============================================================================
from cic_extractor import extract_cic_from_pdf
from credit_rating_analyzer import analyze_credit_accounts

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# FastAPI APP SETUP
# ============================================================================
app = FastAPI(
    title="Financial Document Processor API",
    description="""
    Comprehensive API for processing financial documents:
    
    **Financial Statements:**
    - Extract Balance Sheet and Income Statement from PDFs
    - Uses OCR (Tesseract) for scanned documents
    - Handles conditional processing (processes what exists)
    
    **CIC Documents:**
    - Extract Credit Information Center (CIC) credit account data
    - Uses PDF structure parsing (pdfplumber) for structured tables
    - Includes expert-level credit rating analysis:
      - Repayment status classification
      - Settled loans classification
      - Aggregate credit exposure calculation
    """,
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# PYDANTIC MODELS FOR RESPONSE SCHEMAS
# ============================================================================

class FinancialStatementItem(BaseModel):
    """Individual financial statement line item"""
    label: str
    amount_current: Optional[float] = None
    amount_prior: Optional[float] = None
    group: Optional[str] = None
    note: Optional[str] = None

class FinancialStatement(BaseModel):
    """Financial statement (Balance Sheet or Income Statement)"""
    period_current: Optional[str] = None
    period_prior: Optional[str] = None
    items: List[FinancialStatementItem] = []

class FinancialStatementsResponse(BaseModel):
    """Response model for financial statements extraction"""
    success: bool
    balance_sheet: FinancialStatement
    income_statement: FinancialStatement
    processing_time_seconds: float
    metadata: Dict[str, Any] = {}

class CreditAccountResponse(BaseModel):
    """Individual credit account"""
    institution: str
    account_number: Optional[str] = None
    approved_amount: Optional[str] = None
    current_balance_amount: Optional[str] = None
    date_account_opened: Optional[str] = None
    payment_due_date: Optional[str] = None
    credit_account_risk_classification: Optional[str] = None
    role_in_credit_account: Optional[str] = None
    product_type: Optional[str] = None
    account_status: Optional[str] = None
    repayment_status: Optional[str] = None
    settled_loan_status: Optional[str] = None

class CreditExposureResponse(BaseModel):
    """Credit exposure metrics"""
    total_approved_amount: float
    total_current_balance: float
    total_outstanding: float
    active_accounts_count: int
    settled_accounts_count: int
    total_by_risk_classification: Dict[str, float] = {}
    total_by_product_type: Dict[str, float] = {}
    total_by_institution: Dict[str, float] = {}

class CICResponse(BaseModel):
    """Response model for CIC extraction with credit rating analysis"""
    success: bool
    total_accounts: int
    credit_accounts: List[CreditAccountResponse] = []
    credit_exposure: Optional[CreditExposureResponse] = None
    repayment_status_breakdown: Dict[str, int] = {}
    settled_loans_breakdown: Dict[str, int] = {}
    processing_time_seconds: float
    metadata: Dict[str, Any] = {}

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    service: str
    version: str
    timestamp: str

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _make_statement_amounts_positive(statement: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize statement amounts to be non-negative.
    Used for Financial Statements processing.
    """
    items = statement.get("items", [])
    normalized_items = []
    for item in items:
        new_item = dict(item)
        for key in ("amount_current", "amount_prior"):
            value = new_item.get(key)
            if isinstance(value, (int, float)):
                new_item[key] = abs(float(value))
        normalized_items.append(new_item)
    statement["items"] = normalized_items
    return statement

def _cleanup_temp_file(file_path: str) -> None:
    """Safely remove temporary file"""
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.debug(f"🗑️ Temporary file cleaned up: {file_path}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to cleanup temp file {file_path}: {e}")

# ============================================================================
# FINANCIAL STATEMENTS ENDPOINTS
# ============================================================================

@app.post(
    "/api/v1/financial-statements/extract",
    response_model=FinancialStatementsResponse,
    tags=["Financial Statements"],
    summary="Extract Financial Statements from PDF",
    description="""
    Extract Balance Sheet and Income Statement from a PDF document.
    
    **Processing Flow:**
    1. Save uploaded PDF to temporary file
    2. OCR Processing (Tesseract) - converts PDF pages to text
    3. Statement Extraction - parses Balance Sheet & Income Statement
    
    **Conditional Processing:**
    - If Balance Sheet exists: processes and returns it
    - If Income Statement exists: processes and returns it
    - If either is missing: returns empty structure (does not fail)
    
    **Returns:**
    - Balance Sheet with period_current, period_prior, and items
    - Income Statement with period_current, period_prior, and items
    - Processing time and metadata
    """
)
async def extract_financial_statements(
    file: UploadFile = File(..., description="PDF file containing financial statements"),
    dpi: int = Query(300, ge=100, le=600, description="OCR DPI (100-600)"),
    psm: int = Query(6, ge=0, le=13, description="Tesseract PSM mode (0-13)"),
    conf_threshold: int = Query(10, ge=0, le=100, description="OCR confidence threshold (0-100)")
) -> JSONResponse:
    """
    Extract Financial Statements (Balance Sheet & Income Statement) from PDF.
    """
    start_time = time.time()
    logger.info(f"📄 Starting Financial Statements extraction: {file.filename} ({file.size} bytes)")
    
    # Validate file type
    if file.content_type not in {"application/pdf", "application/octet-stream"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Please upload a PDF file."
        )
    
    tmp_path: str = ""
    try:
        # Step 1: Save uploaded file
        step1_start = time.time()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp_path = tmp.name
            content = await file.read()
            tmp.write(content)
        logger.info(f"✅ Step 1 - File saved: {time.time() - step1_start:.2f}s")

        # Step 2: OCR Processing
        step2_start = time.time()
        logger.info(f"🔍 Step 2 - Starting OCR processing (DPI={dpi}, PSM={psm})...")
        ocr_entries = ocr_pdf_to_lines(tmp_path, dpi=dpi, psm=psm, conf_threshold=conf_threshold)
        logger.info(f"✅ Step 2 - OCR complete: {time.time() - step2_start:.2f}s, {len(ocr_entries)} lines extracted")
        
        # Step 3: Statement Extraction (conditional - handles missing statements)
        step3_start = time.time()
        logger.info("📊 Step 3 - Starting statement extraction...")
        balance, income = extract_statements_from_entries(ocr_entries)
        
        # Normalize amounts to be non-negative
        balance = _make_statement_amounts_positive(balance)
        income = _make_statement_amounts_positive(income)
        
        logger.info(f"✅ Step 3 - Extraction complete: {time.time() - step3_start:.2f}s")
        logger.info(f"   📈 Balance Sheet: {len(balance.get('items', []))} items")
        logger.info(f"   📉 Income Statement: {len(income.get('items', []))} items")
        
        total_time = time.time() - start_time
        logger.info(f"🎉 Total processing time: {total_time:.2f}s")
        
        # Build response
        response = {
            "success": True,
            "balance_sheet": balance,
            "income_statement": income,
            "processing_time_seconds": round(total_time, 2),
            "metadata": {
                "filename": file.filename,
                "file_size_bytes": file.size,
                "ocr_lines_extracted": len(ocr_entries),
                "balance_sheet_items": len(balance.get('items', [])),
                "income_statement_items": len(income.get('items', [])),
                "balance_sheet_periods": {
                    "current": balance.get('period_current'),
                    "prior": balance.get('period_prior')
                },
                "income_statement_periods": {
                    "current": income.get('period_current'),
                    "prior": income.get('period_prior')
                },
                "processing_parameters": {
                    "dpi": dpi,
                    "psm": psm,
                    "conf_threshold": conf_threshold
                }
            }
        }
        
        return JSONResponse(response)
        
    except Exception as e:
        logger.error(f"❌ Error during Financial Statements processing: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Processing error: {str(e)}"
        )
    finally:
        _cleanup_temp_file(tmp_path)

# ============================================================================
# CIC DOCUMENTS ENDPOINTS
# ============================================================================

@app.post(
    "/api/v1/cic/extract",
    response_model=CICResponse,
    tags=["CIC Documents"],
    summary="Extract CIC Credit Account Data from PDF",
    description="""
    Extract Credit Information Center (CIC) credit account data from PDF.
    Includes comprehensive credit rating analysis.
    
    **Processing Flow:**
    1. Save uploaded PDF to temporary file
    2. PDF Structure Parsing (pdfplumber) - extracts Field/Data tables
    3. Credit Account Extraction - parses structured credit account data
    4. Credit Rating Analysis - expert-level classification and exposure calculation
    
    **Credit Rating Analysis Includes:**
    - Repayment Status Classification (for active loans)
    - Settled Loans Classification (for closed accounts)
    - Aggregate Credit Exposure Calculation
    - Risk Classification Breakdown
    - Institution-Level Analysis
    - Product-Type Analysis
    
    **Returns:**
    - Credit accounts with enhanced classifications
    - Credit exposure metrics
    - Repayment status breakdown
    - Settled loans breakdown
    - Processing time and metadata
    """
)
async def extract_cic(
    file: UploadFile = File(..., description="PDF file containing CIC credit account data"),
    include_analysis: bool = Query(True, description="Include credit rating analysis")
) -> JSONResponse:
    """
    Extract CIC credit account data with optional credit rating analysis.
    """
    start_time = time.time()
    logger.info(f"🏦 Starting CIC extraction: {file.filename} ({file.size} bytes)")
    
    # Validate file type
    if file.content_type not in {"application/pdf", "application/octet-stream"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Please upload a PDF file."
        )
    
    tmp_path: str = ""
    try:
        # Step 1: Save uploaded file
        step1_start = time.time()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp_path = tmp.name
            content = await file.read()
            tmp.write(content)
        logger.info(f"✅ Step 1 - File saved: {time.time() - step1_start:.2f}s")

        # Step 2: CIC extraction (uses pdfplumber, no OCR needed)
        step2_start = time.time()
        logger.info("🏦 Step 2 - Starting CIC extraction...")
        result = extract_cic_from_pdf(tmp_path)
        logger.info(f"✅ Step 2 - CIC extraction complete: {time.time() - step2_start:.2f}s")
        
        if not result.get("success"):
            raise HTTPException(
                status_code=400,
                detail=f"CIC extraction failed: {result.get('error', 'Unknown error')}"
            )
        
        logger.info(f"   📊 Total accounts: {result.get('total_accounts', 0)}")
        logger.info(f"   📄 Pages processed: {result.get('start_page', 0)} to {result.get('end_page', 0)}")
        
        # Step 3: Credit Rating Analysis (if requested)
        credit_accounts = result.get("credit_accounts", [])
        analysis_result = None
        
        if include_analysis and credit_accounts:
            step3_start = time.time()
            logger.info("🔍 Step 3 - Starting credit rating analysis...")
            analysis_result = analyze_credit_accounts(credit_accounts)
            logger.info(f"✅ Step 3 - Credit rating analysis complete: {time.time() - step3_start:.2f}s")
            
            # Enhance credit accounts with analysis results
            enhanced_accounts = analysis_result.get("enhanced_accounts", [])
            result["credit_accounts"] = enhanced_accounts
        
        total_time = time.time() - start_time
        logger.info(f"🎉 Total CIC processing time: {total_time:.2f}s")
        
        # Build comprehensive response
        response = {
            "success": True,
            "total_accounts": result.get("total_accounts", 0),
            "credit_accounts": result.get("credit_accounts", []),
            "processing_time_seconds": round(total_time, 2),
            "metadata": {
                "filename": file.filename,
                "file_size_bytes": file.size,
                "pages_processed": result.get("total_pages", 0),
                "start_page": result.get("start_page"),
                "end_page": result.get("end_page"),
                "analysis_included": include_analysis
            }
        }
        
        # Add credit rating analysis if performed
        if analysis_result:
            response["credit_exposure"] = analysis_result.get("credit_exposure", {})
            response["repayment_status_breakdown"] = analysis_result.get("repayment_status_breakdown", {})
            response["settled_loans_breakdown"] = analysis_result.get("settled_loans_breakdown", {})
            response["coopbank_grading"] = analysis_result.get("coopbank_grading", {})
            response["metadata"]["active_accounts"] = analysis_result.get("active_accounts", 0)
            response["metadata"]["settled_accounts"] = analysis_result.get("settled_accounts", 0)
        
        return JSONResponse(response)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error during CIC processing: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"CIC processing error: {str(e)}"
        )
    finally:
        _cleanup_temp_file(tmp_path)

# ============================================================================
# HEALTH & UTILITY ENDPOINTS
# ============================================================================

@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["Health"],
    summary="Health Check",
    description="Check API health and service status"
)
async def health() -> JSONResponse:
    """Health check endpoint"""
    return JSONResponse({
        "status": "healthy",
        "service": "financial-document-processor",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat()
    })

@app.get(
    "/api/v1/info",
    tags=["Info"],
    summary="API Information",
    description="Get API information and capabilities"
)
async def api_info() -> JSONResponse:
    """Get API information"""
    return JSONResponse({
        "service": "Financial Document Processor API",
        "version": "2.0.0",
        "capabilities": {
            "financial_statements": {
                "endpoint": "/api/v1/financial-statements/extract",
                "description": "Extract Balance Sheet and Income Statement from PDFs",
                "method": "POST",
                "requires_ocr": True,
                "supports_conditional_processing": True
            },
            "cic_documents": {
                "endpoint": "/api/v1/cic/extract",
                "description": "Extract CIC credit account data with credit rating analysis",
                "method": "POST",
                "requires_ocr": False,
                "credit_rating_analysis": True
            }
        },
        "features": [
            "Conditional processing (handles missing statements)",
            "Credit rating analysis (repayment status, settled loans)",
            "Aggregate credit exposure calculation",
            "Risk classification breakdown",
            "Institution-level analysis"
        ]
    })

# ============================================================================
# LEGACY ENDPOINTS (for backward compatibility)
# ============================================================================

@app.post(
    "/extract",
    tags=["Legacy"],
    summary="[Legacy] Extract Financial Statements",
    description="Legacy endpoint for financial statements extraction. Use /api/v1/financial-statements/extract instead."
)
async def extract_legacy(file: UploadFile = File(...)) -> JSONResponse:
    """Legacy endpoint - redirects to new endpoint"""
    return await extract_financial_statements(file)

@app.post(
    "/extract-cic",
    tags=["Legacy"],
    summary="[Legacy] Extract CIC",
    description="Legacy endpoint for CIC extraction. Use /api/v1/cic/extract instead."
)
async def extract_cic_legacy(file: UploadFile = File(...)) -> JSONResponse:
    """Legacy endpoint - redirects to new endpoint"""
    return await extract_cic(file)

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    
    logger.info(f"🚀 Starting Financial Document Processor API on {host}:{port}")
    logger.info(f"📚 API Documentation available at http://{host}:{port}/docs")
    
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info"
    )


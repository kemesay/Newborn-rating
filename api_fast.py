from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
import tempfile
import os
import time
import logging
from typing import Dict, Any

from ocr_to_lines import ocr_pdf_to_lines
from extract_statements import extract_statements_from_entries
from cic_extractor import extract_cic_from_pdf

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Financial Statement Extractor - Fast Version")


@app.post("/extract")
async def extract(file: UploadFile = File(...)) -> JSONResponse:
    start_time = time.time()
    logger.info(f"📄 Starting extraction for file: {file.filename}")
    
    if file.content_type not in {"application/pdf", "application/octet-stream"}:
        raise HTTPException(status_code=400, detail="Please upload a PDF file")
    
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
        logger.info("🔍 Step 2 - Starting OCR processing...")
        ocr_entries = ocr_pdf_to_lines(tmp_path, dpi=300, psm=6, conf_threshold=10)
        logger.info(f"✅ Step 2 - OCR complete: {time.time() - step2_start:.2f}s, {len(ocr_entries)} lines extracted")

        # Step 3: Statement Extraction
        step3_start = time.time()
        logger.info("📊 Step 3 - Starting statement extraction...")
        balance, income = extract_statements_from_entries(ocr_entries)
        logger.info(f"✅ Step 3 - Extraction complete: {time.time() - step3_start:.2f}s")
        logger.info(f"   📈 Balance Sheet: {len(balance.get('items', []))} items")
        logger.info(f"   📉 Income Statement: {len(income.get('items', []))} items")
        
        total_time = time.time() - start_time
        logger.info(f"🎉 Total processing time: {total_time:.2f}s")
        
        return JSONResponse(
            {
                "balance_sheet": balance,
                "income_statement": income,
                "processing_time_seconds": round(total_time, 2)
            }
        )
    except Exception as e:
        logger.error(f"❌ Error during processing: {e}")
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
                logger.info("🗑️ Temporary file cleaned up")
            except Exception:
                pass


@app.post("/extract-cic")
async def extract_cic(file: UploadFile = File(...)) -> JSONResponse:
    start_time = time.time()
    logger.info(f"🏦 Starting CIC extraction for file: {file.filename}")
    
    if file.content_type not in {"application/pdf", "application/octet-stream"}:
        raise HTTPException(status_code=400, detail="Please upload a PDF file")
    
    tmp_path: str = ""
    try:
        # Step 1: Save uploaded file
        step1_start = time.time()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp_path = tmp.name
            content = await file.read()
            tmp.write(content)
        logger.info(f"✅ Step 1 - File saved: {time.time() - step1_start:.2f}s")

        # Step 2: CIC extraction (no OCR needed - uses pdfplumber)
        step2_start = time.time()
        logger.info("🏦 Step 2 - Starting CIC extraction...")
        result = extract_cic_from_pdf(tmp_path)
        logger.info(f"✅ Step 2 - CIC extraction complete: {time.time() - step2_start:.2f}s")
        
        if result.get("success"):
            logger.info(f"   📊 Total accounts: {result.get('total_accounts', 0)}")
            logger.info(f"   📄 Pages processed: {result.get('start_page', 0)} to {result.get('end_page', 0)}")
        
        total_time = time.time() - start_time
        logger.info(f"🎉 Total CIC processing time: {total_time:.2f}s")
        
        return JSONResponse(result)
        
    except Exception as e:
        logger.error(f"❌ Error during CIC processing: {e}")
        raise HTTPException(status_code=500, detail=f"CIC processing error: {str(e)}")
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
                logger.info("🗑️ Temporary file cleaned up")
            except Exception:
                pass


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "financial-extractor-fast"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8001)))


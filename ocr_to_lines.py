import argparse
import json
from typing import List, Dict, Any, Tuple

import numpy as np
import pytesseract
from pdf2image import convert_from_path
import pandas as pd
import re


def _clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s)
    # Strip stray pipes/underscores and repeated punctuation at edges
    s = re.sub(r"^[\|_\-\s]+", "", s)
    s = re.sub(r"[\|_\-\s]+$", "", s)
    return s.strip()


def _group_tokens_by_y(ocr_df: pd.DataFrame) -> List[List[pd.Series]]:
    # Robust line grouping by y-center proximity
    df = ocr_df.copy()
    if "top" not in df.columns or "height" not in df.columns or "left" not in df.columns:
        # Fallback to naive grouping by line_num
        groups = []
        group_cols = [c for c in ["block_num", "par_num", "line_num"] if c in df.columns]
        if not group_cols:
            group_cols = ["line_num"]
        for _, g in df.groupby(group_cols, dropna=False):
            rows = [r for _, r in g.sort_values(by="left").iterrows()]
            if rows:
                groups.append(rows)
        return groups

    df["y_center"] = df["top"].astype(float) + df["height"].astype(float) / 2.0
    df = df.sort_values(by="y_center")
    median_h = float(df["height"].median()) if len(df) else 12.0
    y_gap = max(8.0, 0.7 * median_h)

    groups: List[List[pd.Series]] = []
    current: List[pd.Series] = []
    last_y: float = -1e9
    for _, row in df.iterrows():
        y = float(row["y_center"])  # type: ignore
        if y - last_y > y_gap and current:
            # start new line
            groups.append(sorted(current, key=lambda r: float(r["left"])) )
            current = []
        current.append(row)
        last_y = y
    if current:
        groups.append(sorted(current, key=lambda r: float(r["left"])) )
    return groups


def _df_to_lines(df: pd.DataFrame) -> List[str]:
    # Convert tokens to line strings via y-clustering
    line_tokens = _group_tokens_by_y(df)
    lines: List[str] = []
    for tokens in line_tokens:
        parts = [str(t["text"]) for t in tokens if str(t["text"]).strip()]
        sent = _clean_text(" ".join(parts))
        if sent:
            lines.append(sent)
    return lines


def _score_lines(lines: List[str]) -> float:
    if not lines:
        return -1e9
    total = len(lines)
    long = sum(1 for s in lines if len(s) > 140)
    chars = sum(len(s) for s in lines)
    # Prefer more lines, fewer very long ones
    return total - 1.5 * (long / max(1, total)) - 0.0005 * chars


def ocr_pdf_to_lines(
    pdf_path: str,
    dpi: int = 300,
    psm: int = 6,
    conf_threshold: int = 10,
) -> List[Dict[str, Any]]:
    import logging
    logger = logging.getLogger(__name__)
    
    import time
    start_time = time.time()
    
    logger.info(f"📖 Converting PDF to images at {dpi} DPI...")
    pages = convert_from_path(pdf_path, dpi=dpi)
    logger.info(f"✅ PDF converted: {len(pages)} pages in {time.time() - start_time:.2f}s")
    
    all_lines_json: List[Dict[str, Any]] = []
    
    for page_index, page in enumerate(pages, start=1):
        page_start = time.time()
        logger.info(f"🔍 Processing page {page_index}/{len(pages)}...")
        
        image_np = np.array(page)

        # Start with preferred PSM, try fallbacks only if needed
        candidate_lines_per_psm: List[Tuple[int, List[str]]] = []
        
        # Try primary PSM first
        ocr_df = pytesseract.image_to_data(
            image_np,
            lang="eng",
            config=fr"--oem 3 --psm {psm}",
            output_type=pytesseract.Output.DATAFRAME,
        )
        ocr_df = ocr_df.dropna(subset=["text"])  # type: ignore
        if "conf" in ocr_df.columns:
            try:
                ocr_df = ocr_df[ocr_df["conf"].astype(float) >= conf_threshold]
            except Exception:
                pass
        lines = _df_to_lines(ocr_df)
        candidate_lines_per_psm.append((psm, lines))
        
        # Only try fallbacks if primary result is poor (very few lines or very long lines)
        score = _score_lines(lines)
        if score < 5 or len(lines) < 10:  # Poor result, try fallbacks
            logger.info(f"  🔄 Primary PSM {psm} gave poor results, trying fallbacks...")
            for trial_psm in {11, 4}:  # Reduced fallback set
                if trial_psm != psm:
                    ocr_df = pytesseract.image_to_data(
                        image_np,
                        lang="eng",
                        config=fr"--oem 3 --psm {trial_psm}",
                        output_type=pytesseract.Output.DATAFRAME,
                    )
                    ocr_df = ocr_df.dropna(subset=["text"])  # type: ignore
                    if "conf" in ocr_df.columns:
                        try:
                            ocr_df = ocr_df[ocr_df["conf"].astype(float) >= conf_threshold]
                        except Exception:
                            pass
                    fallback_lines = _df_to_lines(ocr_df)
                    candidate_lines_per_psm.append((trial_psm, fallback_lines))

        # Pick best candidate by heuristic score
        best_psm, best_lines = max(candidate_lines_per_psm, key=lambda t: _score_lines(t[1]))
        
        logger.info(f"  ✅ Page {page_index}: {len(best_lines)} lines with PSM {best_psm} in {time.time() - page_start:.2f}s")

        for sentence in best_lines:
            all_lines_json.append({"page": page_index, "line": sentence})
    
    total_time = time.time() - start_time
    logger.info(f"🎉 OCR complete: {len(all_lines_json)} total lines in {total_time:.2f}s")
    return all_lines_json


def main() -> None:
    parser = argparse.ArgumentParser(description="OCR a PDF into lines JSON using Tesseract and pdf2image.")
    parser.add_argument("--pdf", required=True, help="Path to the PDF file")
    parser.add_argument("--out", required=True, help="Path to write lines JSON")
    parser.add_argument("--dpi", type=int, default=300, help="Rasterization DPI (default: 300)")
    args = parser.parse_args()

    lines = ocr_pdf_to_lines(args.pdf, dpi=args.dpi)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(lines, f, ensure_ascii=False, indent=2)
    print(f"Saved OCR lines to {args.out}")


if __name__ == "__main__":
    main()



import argparse
import os
import json

from ocr_to_lines import ocr_pdf_to_lines
from extract_statements import extract_statements


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end: PDF -> OCR lines -> financial statements JSON")
    parser.add_argument("--pdf", required=True, help="Path to PDF input")
    parser.add_argument("--lines-json", required=False, default=None, help="Optional path to save intermediate OCR lines JSON")
    parser.add_argument("--balance-json", required=True, help="Path to output balance sheet JSON")
    parser.add_argument("--income-json", required=True, help="Path to output income statement JSON")
    parser.add_argument("--dpi", type=int, default=300, help="Rasterization DPI for OCR (default: 300)")
    args = parser.parse_args()
    # 1) OCR
    lines = ocr_pdf_to_lines(args.pdf, dpi=args.dpi)
    if args.lines_json:
        os.makedirs(os.path.dirname(args.lines_json), exist_ok=True)
        with open(args.lines_json, "w", encoding="utf-8") as f:
            json.dump(lines, f, ensure_ascii=False, indent=2)
            
    # 2) Extract
    # Write to a temp path if no lines-json provided
    lines_path = args.lines_json
    if not lines_path:
        base_dir = "/tmp"
        lines_path = os.path.join(base_dir, "ocr_lines.json")
        with open(lines_path, "w", encoding="utf-8") as f:
            json.dump(lines, f, ensure_ascii=False, indent=2)

    balance, income = extract_statements(lines_path)

    os.makedirs(os.path.dirname(args.balance_json), exist_ok=True)
    with open(args.balance_json, "w", encoding="utf-8") as f:
        json.dump(balance, f, ensure_ascii=False, indent=2)

    os.makedirs(os.path.dirname(args.income_json), exist_ok=True)
    with open(args.income_json, "w", encoding="utf-8") as f:
        json.dump(income, f, ensure_ascii=False, indent=2)

    print(f"Wrote balance: {args.balance_json}\nWrote income: {args.income_json}")


if __name__ == "__main__":
    main()



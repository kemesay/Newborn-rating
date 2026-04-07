import pdfplumber
import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class CreditAccount:
    """Data class for credit account information"""
    institution: str = ""
    secured_guarantor_total: str = ""
    date_account_opened: str = ""
    current_balance_amount: str = ""
    payment_due_date: str = ""
    credit_account_risk_classification: str = ""
    role_in_credit_account: str = ""
    product_type: str = ""
    account_number: str = ""
    office: str = ""
    approved_amount: str = ""
    credit_type: str = ""
    maturity_date: str = ""

def find_credit_account_detail_section(pdf_path: str) -> Tuple[int, int]:
    """
    Intelligently find the start and end pages of the 'Credit Account Detail' section.
    
    Returns:
        Tuple[int, int]: (start_page, end_page) - 1-indexed page numbers
    """
    logger.info(f"🔍 Searching for 'Credit Account Detail' section in {pdf_path}")
    
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        logger.info(f"📄 Total pages: {total_pages}")
        
        # Strategy 1: Look for the actual data structure (Field/Data tables)
        # This is more reliable than just text search
        start_page = None
        
        # Search from the beginning, but prioritize later pages for efficiency
        search_order = []
        
        # First check pages 1-20 for any early occurrences
        search_order.extend(range(1, min(21, total_pages + 1)))
        
        # Then check middle section (most common location)
        if total_pages > 40:
            mid_start = max(40, total_pages // 3)
            mid_end = min(total_pages - 20, total_pages * 2 // 3)
            search_order.extend(range(mid_start, mid_end + 1))
        
        # Finally check remaining pages
        remaining_pages = [p for p in range(1, total_pages + 1) if p not in search_order]
        search_order.extend(remaining_pages)
        
        # Search for the actual data structure
        for page_num in search_order:
            page = pdf.pages[page_num - 1]
            text = page.extract_text()
            
            # Look for the actual data structure: "Field Data" pattern
            if "Field Data" in text and any(keyword in text for keyword in [
                "Institution", "Account Number", "Role in Credit Account"
            ]):
                start_page = page_num
                logger.info(f"✅ Found Credit Account Detail data structure at page {start_page}")
                break
        
        # Fallback: If no data structure found, look for the text "Credit Account Detail"
        if not start_page:
            logger.info("🔍 No data structure found, searching for 'Credit Account Detail' text...")
            for page_num in range(1, total_pages + 1):
                page = pdf.pages[page_num - 1]
                text = page.extract_text()
                
                if "Credit Account Detail" in text:
                    start_page = page_num
                    logger.info(f"✅ Found 'Credit Account Detail' text at page {start_page}")
                    break
        
        if not start_page:
            logger.warning("⚠️ Credit Account Detail section not found")
            return None, None
        
        # Find the end of the section by looking for the last page with structured data
        end_page = start_page
        
        # Look forward up to 50 pages to find where the structured data ends
        for page_num in range(start_page, min(start_page + 50, total_pages + 1)):
            page = pdf.pages[page_num - 1]
            text = page.extract_text()
            
            # Check if this page still has structured credit account data
            if ("Field Data" in text and any(keyword in text for keyword in [
                "Institution", "Account Number", "Role in Credit Account"
            ])):
                end_page = page_num
            # Look for other major sections that might indicate end of credit accounts
            elif any(keyword in text for keyword in [
                "Credit Summary", "Credit History", "Credit Report", 
                "End of Report", "Report Generated", "Total Accounts",
                "Credit Score", "Credit Rating"
            ]):
                break
        
        logger.info(f"📊 Credit Account Detail section: pages {start_page} to {end_page}")
        return start_page, end_page

def extract_credit_accounts_from_pages(pdf_path: str, start_page: int, end_page: int) -> List[CreditAccount]:
    """
    Extract credit account information from the specified page range.
    
    Args:
        pdf_path: Path to the PDF file
        start_page: Starting page number (1-indexed)
        end_page: Ending page number (1-indexed)
    
    Returns:
        List[CreditAccount]: List of extracted credit account objects
    """
    logger.info(f"📊 Extracting credit accounts from pages {start_page} to {end_page}")
    
    credit_accounts = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num in range(start_page, end_page + 1):
            if page_num > len(pdf.pages):
                break
                
            page = pdf.pages[page_num - 1]
            logger.info(f"  🔍 Processing page {page_num}")
            
            # Extract tables from the page
            tables = page.extract_tables()
            
            for table_num, table in enumerate(tables):
                if not table or len(table) < 2:
                    continue
                
                # Look for tables with "Field" and "Data" structure
                if any("Field" in str(cell) for cell in table[0]):
                    logger.info(f"    📋 Found structured table {table_num + 1}")
                    
                    # Parse the table to extract account information
                    account = parse_credit_account_table(table)
                    if account and account.institution:  # Only add if we have basic info
                        credit_accounts.append(account)
                        logger.info(f"      ✅ Extracted account: {account.institution} - {account.account_number}")
    
    logger.info(f"🎉 Total credit accounts extracted: {len(credit_accounts)}")
    return credit_accounts

def parse_credit_account_table(table: List[List[str]]) -> Optional[CreditAccount]:
    """
    Parse a table with Field/Data structure to extract credit account information.
    
    Args:
        table: List of rows, each row is a list of cells
    
    Returns:
        CreditAccount object or None if parsing fails
    """
    account = CreditAccount()
    
    # Create a mapping of field names to their values
    field_data = {}
    
    for row in table:
        if len(row) >= 2:
            field = str(row[0]).strip() if row[0] else ""
            data = str(row[1]).strip() if len(row) > 1 and row[1] else ""
            
            if field and data and field != "Field":
                field_data[field.lower()] = data
    
    
    # Map the extracted fields to our CreditAccount object
    if "institution" in field_data:
        account.institution = field_data["institution"]
    
    if "secured by guarantor total" in field_data:
        account.secured_guarantor_total = field_data["secured by guarantor total"]
    
    if "date account opened" in field_data:
        account.date_account_opened = field_data["date account opened"]
    
    if "current balance amount" in field_data:
        account.current_balance_amount = field_data["current balance amount"]
    # elif "opening balance / credit limit" in field_data:
    #     account.current_balance_amount = field_data["opening balance / credit limit"]
    
    if "payment due date" in field_data:
        account.payment_due_date = field_data["payment due date"]
    
    if "credit account risk classification" in field_data:
        account.credit_account_risk_classification = field_data["credit account risk classification"]
    
    if "role in credit account" in field_data:
        account.role_in_credit_account = field_data["role in credit account"]
    
    if "product type" in field_data:
        account.product_type = field_data["product type"]
    
    if "account number" in field_data:
        account.account_number = field_data["account number"]
    
    if "office" in field_data:
        account.office = field_data["office"]
    
    if "approved amount" in field_data:
        account.approved_amount = field_data["approved amount"]
    
    if "credit type" in field_data:
        account.credit_type = field_data["credit type"]
    
    if "maturity date" in field_data:
        account.maturity_date = field_data["maturity date"]
    
    return account

def extract_cic_from_pdf(pdf_path: str) -> Dict[str, Any]:
    """
    Main function to extract CIC data from a PDF file.
    
    Args:
        pdf_path: Path to the PDF file
    
    Returns:
        Dict containing extracted credit accounts and metadata
    """
    logger.info(f"🚀 Starting CIC extraction from: {pdf_path}")
    
    try:
        # Step 1: Find the Credit Account Detail section
        start_page, end_page = find_credit_account_detail_section(pdf_path)
        
        if not start_page:
            return {
                "success": False,
                "error": "Credit Account Detail section not found",
                "credit_accounts": []
            }
        
        # Step 2: Extract credit accounts from the identified pages
        credit_accounts = extract_credit_accounts_from_pages(pdf_path, start_page, end_page)
        
        # Step 3: Convert to serializable format
        accounts_data = []
        for account in credit_accounts:
            accounts_data.append({
                "institution": account.institution,
                "secured_guarantor_total": account.secured_guarantor_total,
                "date_account_opened": account.date_account_opened,
                "current_balance_amount": account.current_balance_amount,
                "payment_due_date": account.payment_due_date,
                "credit_account_risk_classification": account.credit_account_risk_classification,
                "role_in_credit_account": account.role_in_credit_account,
                "product_type": account.product_type,
                "account_number": account.account_number,
                "office": account.office,
                "approved_amount": account.approved_amount,
                "credit_type": account.credit_type,
                "maturity_date": account.maturity_date
            })
        
        result = {
            "success": True,
            "pdf_path": pdf_path,
            "total_pages": end_page - start_page + 1 if start_page and end_page else 0,
            "start_page": start_page,
            "end_page": end_page,
            "total_accounts": len(credit_accounts),
            "credit_accounts": accounts_data
        }
        
        logger.info(f"✅ CIC extraction completed successfully: {len(credit_accounts)} accounts")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error during CIC extraction: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "credit_accounts": []
        }

if __name__ == "__main__":
    # Test the extraction
    test_pdf = "/home/name-1/AI-Agent/new-born/data/HABESHA PETROLEUM AND (1)/ABATHNE CHANE AYLEW.pdf"
    result = extract_cic_from_pdf(test_pdf)
    
    if result["success"]:
        print(f"\n✅ Extraction successful!")
        print(f"📄 Pages processed: {result['start_page']} to {result['end_page']}")
        print(f"🏦 Total accounts: {result['total_accounts']}")
        
        for i, account in enumerate(result['credit_accounts'][:3], 1):  # Show first 3
            print(f"\n--- Account {i} ---")
            print(f"Institution: {account['institution']}")
            print(f"Account: {account['account_number']}")
            print(f"Product: {account['product_type']}")
            print(f"Amount: {account['approved_amount']}")
    else:
        print(f"❌ Extraction failed: {result['error']}")

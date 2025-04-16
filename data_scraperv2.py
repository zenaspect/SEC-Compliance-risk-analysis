import os
import requests
import pandas as pd
import time
from urllib.parse import urljoin
import json
from datetime import datetime

# Configuration
COMPANIES = {
    "AAPL": "0000320193",  # Apple
    "MSFT": "0000789019",  # Microsoft
    "GOOGL": "0001652044", # Alphabet
}
OUTPUT_DIR = "sec_data"
USER_AGENT = "Your Company Name your@email.com"
TRACKING_FILE = f"{OUTPUT_DIR}/metadata/filings_tracker.csv"

def setup_directories():
    """Create organized directory structure and initialize tracking file if needed"""
    os.makedirs(f"{OUTPUT_DIR}/metadata", exist_ok=True)
    
    # Initialize tracking file if it doesn't exist
    if not os.path.exists(TRACKING_FILE):
        pd.DataFrame(columns=[
            'ticker', 'cik', 'accessionNumber', 'form', 'filingDate', 
            'reportDate', 'localPath', 'downloadTime', 'fileSize'
        ]).to_csv(TRACKING_FILE, index=False)

def load_existing_tracking():
    """Load existing tracking data to avoid duplicates"""
    if os.path.exists(TRACKING_FILE):
        return pd.read_csv(TRACKING_FILE)
    return pd.DataFrame()

def update_tracking_file(new_entries):
    """Update the tracking file with new entries"""
    existing = load_existing_tracking()
    updated = pd.concat([existing, pd.DataFrame(new_entries)], ignore_index=True)
    
    # Remove duplicates (in case we're re-running the script)
    updated = updated.drop_duplicates(
        subset=['cik', 'accessionNumber'], 
        keep='last'
    )
    
    updated.to_csv(TRACKING_FILE, index=False)
    return updated

def fetch_company_submissions(cik):
    """Fetch all submissions metadata for a company"""
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching submissions for CIK {cik}: {str(e)}")
        return None

def download_filing(ticker, cik, accession_number, form_type, filing_date):
    """Download filing and return metadata"""
    base_url = "https://www.sec.gov/Archives/"
    accession_clean = accession_number.replace("-", "")
    filing_url = f"{base_url}edgar/data/{cik}/{accession_clean}/{accession_number}.txt"
    
    try:
        response = requests.get(filing_url, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        
        # Create organized file path
        file_dir = f"{OUTPUT_DIR}/{ticker}/{form_type}"
        os.makedirs(file_dir, exist_ok=True)
        filename = f"{file_dir}/{filing_date}_{accession_clean}.txt"
        
        # Save file
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        # Get file stats
        file_size = os.path.getsize(filename)
        
        return {
            'filename': filename,
            'file_size': file_size,
            'download_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        print(f"Error downloading filing {accession_number}: {str(e)}")
        return None

def main():
    setup_directories()
    existing_tracker = load_existing_tracking()
    new_entries = []
    
    for ticker, cik in COMPANIES.items():
        print(f"\nProcessing {ticker} (CIK: {cik})")
        
        # Get company submissions
        submissions = fetch_company_submissions(cik)
        if not submissions:
            continue
        
        # Process recent filings
        filings = submissions.get('filings', {}).get('recent', {})
        for i in range(len(filings['accessionNumber'])):
            accession = filings['accessionNumber'][i]
            form_type = filings['form'][i]
            filing_date = filings['filingDate'][i]
            
            # Skip if we already have this filing
            if not existing_tracker.empty:
                exists = ((existing_tracker['cik'] == cik) & 
                         (existing_tracker['accessionNumber'] == accession)).any()
                if exists:
                    print(f"Skipping already downloaded {form_type} {accession}")
                    continue
            
            if form_type in ['10-K', '10-Q', '8-K']:
                print(f"Downloading {form_type} filed on {filing_date}...")
                
                # Download filing
                result = download_filing(ticker, cik, accession, form_type, filing_date)
                if not result:
                    continue
                
                # Prepare tracking entry
                entry = {
                    'ticker': ticker,
                    'cik': cik,
                    'accessionNumber': accession,
                    'form': form_type,
                    'filingDate': filing_date,
                    'reportDate': filings['reportDate'][i],
                    'localPath': result['filename'],
                    'downloadTime': result['download_time'],
                    'fileSize': result['file_size']
                }
                new_entries.append(entry)
                time.sleep(0.2)  # Respect rate limits
    
    # Update tracking file if we have new entries
    if new_entries:
        update_tracking_file(new_entries)
        print(f"\nAdded {len(new_entries)} new filings to tracker")
    else:
        print("\nNo new filings found to download")

if __name__ == "__main__":
    main()
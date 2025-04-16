import os
import re
import csv
import time
import pandas as pd
from textblob import TextBlob
from datetime import datetime

# Configuration
PARSED_DATA_DIR = "C:/codes/Data_Analytics/sec_data_cleaned"
OUTPUT_FILE = "C:/codes/Data_Analytics/risk_scores2.csv"
CHECKPOINT_FILE = "C:/codes/Data_Analytics/processing_checkpoints.csv"
LOG_FILE = "C:/codes/Data_Analytics/processing.log"

# Risk keywords with weights
RISK_KEYWORDS = {
    'litigation': 3, 'lawsuit': 3, 'breach': 4, 'cybersecurity': 4,
    'fraud': 5, 'regulation': 3, 'compliance': 3, 'debt': 2,
    'competition': 2, 'pandemic': 4, 'recession': 3, 'default': 5
}

def setup_logging():
    """Initialize log files"""
    with open(LOG_FILE, 'w') as f:
        f.write(f"SEC Risk Analysis Log - {datetime.now()}\n\n")
    with open(CHECKPOINT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'filepath', 'status', 'risk_score'])

def log_message(message):
    """Write to both console and log file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'a') as f:
        f.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

def log_checkpoint(filepath, status, risk_score=None):
    """Record processing checkpoint"""
    with open(CHECKPOINT_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            filepath,
            status,
            risk_score or ''
        ])

def calculate_risk_score(text):
    """Calculate risk score with validation"""
    if not text or len(text.strip()) < 100:
        return 0
    
    try:
        # Keyword analysis (60%)
        words = re.findall(r'\w+', text.lower())
        kw_score = min(60, sum(words.count(w)*wt for w,wt in RISK_KEYWORDS.items()) * 0.6)
        
        # Sentiment analysis (40%)
        sentiment = TextBlob(text).sentiment
        sent_score = 40 * (1 - (sentiment.polarity + 1)/2)
        
        return min(100, max(1, int(kw_score + sent_score)))
    except Exception as e:
        log_message(f"Scoring error: {str(e)}")
        return 0

def parse_filing_date(filename):
    """Extract and format filing date from filename"""
    try:
        # Extract date part (assumes format YYYY-MM-DD_XXXXXXXX.txt)
        date_str = filename.split('_')[0]
        
        # Parse and reformat to ensure YYYY-MM-DD
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError as e:
        log_message(f"Date parsing error for {filename}: {str(e)}")
        return date_str  # Return original if parsing fails

def process_single_file(filepath):
    """Process one filing with checkpoint logging"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            text = f.read().strip()
        
        if not text:
            log_checkpoint(filepath, "EMPTY")
            return None

        # Extract metadata from path
        parts = filepath.split(os.sep)
        ticker = parts[-3]
        form_type = parts[-2]
        filename = os.path.splitext(parts[-1])[0]
        
        # Get properly formatted filing date
        filing_date = parse_filing_date(filename)
        
        # Calculate risk
        risk_score = calculate_risk_score(text)
        log_checkpoint(filepath, "PROCESSED", risk_score)
        
        return {
            'ticker': ticker,
            'form': form_type,
            'filing_date': filing_date,  # Formatted YYYY-MM-DD
            'file_path': filepath,
            'risk_score': risk_score,
            'risk_category': (
                'Very High' if risk_score >= 80 else
                'High' if risk_score >= 60 else
                'Moderate' if risk_score >= 40 else
                'Low' if risk_score >= 20 else 'Very Low'
            )
        }
    except Exception as e:
        log_message(f"Failed {filepath}: {str(e)}")
        log_checkpoint(filepath, "FAILED")
        return None

def analyze_filings():
    """Main processing function with progress tracking"""
    setup_logging()
    results = []
    total_files = 0
    processed_files = 0
    
    # Count files first
    log_message("Counting files...")
    for root, _, files in os.walk(PARSED_DATA_DIR):
        total_files += sum(1 for f in files if f.endswith('.txt'))
    log_message(f"Found {total_files} filings to process")
    
    # Process files
    start_time = time.time()
    for root, _, files in os.walk(PARSED_DATA_DIR):
        for file in files:
            if file.endswith('.txt'):
                filepath = os.path.join(root, file)
                
                # Process with checkpoint
                result = process_single_file(filepath)
                if result:
                    results.append(result)
                    processed_files += 1
                
                # Progress update
                if processed_files % 10 == 0 or processed_files == total_files:
                    elapsed = time.time() - start_time
                    remaining = (elapsed/processed_files)*(total_files-processed_files) if processed_files else 0
                    log_message(
                        f"Progress: {processed_files}/{total_files} "
                        f"({processed_files/total_files:.1%}) | "
                        f"Elapsed: {elapsed:.1f}s | "
                        f"ETA: {remaining:.1f}s"
                    )
    
    # Save results
    if results:
        df = pd.DataFrame(results)
        
        # Ensure proper date format in final output
        df['filing_date'] = pd.to_datetime(df['filing_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        df.to_csv(OUTPUT_FILE, index=False)
        log_message(f"Saved {len(results)} records to {OUTPUT_FILE}")
        
        # Print summary
        log_message("\nRisk Score Distribution:")
        log_message(str(df['risk_category'].value_counts()))
        
        log_message("\nTop 5 Riskiest Filings:")
        for _, row in df.nlargest(5, 'risk_score').iterrows():
            log_message(f"{row['ticker']} {row['form']} ({row['filing_date']}): {row['risk_score']}/100")
    else:
        log_message("⚠️ No files processed successfully")

if __name__ == "__main__":
    print("Starting SEC risk analysis...")
    analyze_filings()
    print(f"Detailed log saved to: {LOG_FILE}")
    print(f"Checkpoint records saved to: {CHECKPOINT_FILE}")
    print(f"Risk scores saved to: {OUTPUT_FILE}")
import os
import re
import csv
from datetime import datetime

def clean_sec_file(input_path, output_path):
    """Remove HTML tags and clean up text formatting"""
    with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # Remove HTML tags but preserve line breaks
    clean_content = re.sub(r'<[^>]+>', '\n', content)
    
    # Remove excessive whitespace but keep paragraphs
    clean_content = re.sub(r'\n\s+\n', '\n\n', clean_content)
    clean_content = clean_content.strip()
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(clean_content)

def process_sec_files(input_dir, output_dir):
    """Process all files while maintaining directory structure"""
    os.makedirs(output_dir, exist_ok=True)
    
    # Create tracking log
    log_file = os.path.join(output_dir, 'processing_log.csv')
    with open(log_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Filepath', 'Status', 'Timestamp'])
    
    # Process each file
    for root, _, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith('.txt'):
                input_path = os.path.join(root, filename)
                rel_path = os.path.relpath(root, input_dir)
                output_path = os.path.join(output_dir, rel_path, filename)
                
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                try:
                    clean_sec_file(input_path, output_path)
                    status = 'SUCCESS'
                    print(f"Cleaned: {input_path}")
                except Exception as e:
                    status = f'FAILED: {str(e)}'
                    print(f"Error processing {input_path}: {str(e)}")
                
                # Log results
                with open(log_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([input_path, status, datetime.now().isoformat()])

if __name__ == "__main__":
    INPUT_DIR = "C:/codes/Data_Analytics/sec_data"
    OUTPUT_DIR = "C:/codes/Data_Analytics/sec_data_cleaned"
    
    print("Starting SEC files cleaning...")
    process_sec_files(INPUT_DIR, OUTPUT_DIR)
    print(f"\nDone! Cleaned files saved to: {OUTPUT_DIR}")
    print(f"Processing log: {os.path.join(OUTPUT_DIR, 'processing_log.csv')}")
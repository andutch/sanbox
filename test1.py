import os
import requests
import re
import time
from tqdm import tqdm

def check_available_quarters(base_url: str, year: int, session):
    available_quarters = []
    quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
    
    for quarter in quarters:
        url = f"{base_url}/{year}/{quarter}/form.idx"
        response = session.head(url)
        if response.status_code == 200:
            available_quarters.append(quarter)
        else:
            print(f"{quarter} is not available for {year}.")
    
    return available_quarters

def save_transactions_batch(transactions_batch, base_dir, session, progress_bar):
    total_data_transferred = 0  # Track the total amount of data transferred in bytes
    for transaction in transactions_batch:
        cik = transaction['cik']
        file_name = transaction['file_name']
        url = f"https://www.sec.gov/Archives/{file_name}"
        
        cik_dir = os.path.join(base_dir, cik)
        if not os.path.exists(cik_dir):
            os.makedirs(cik_dir)
        
        file_path = os.path.join(cik_dir, os.path.basename(file_name))
        
        if os.path.exists(file_path):
            local_file_size = os.path.getsize(file_path)
            response = session.head(url)
            remote_file_size = int(response.headers.get('Content-Length', 0))
            
            if local_file_size == remote_file_size:
                progress_bar.update(1)
                continue
        
        response = session.get(url)
        time.sleep(0.1)
        if response.status_code == 200:
            with open(file_path, 'w') as f:
                f.write(response.text)
            total_data_transferred += len(response.content)
        
        progress_bar.update(1)
    
    return total_data_transferred

def process_transactions(base_url: str, start_year: int, end_year: int, base_dir: str, batch_size: int = 100):
    transactions_batch = []
    total_data_transferred = 0
    
    with requests.Session() as session:
        session.headers.update({
            'User-Agent': 'my-app-name/1.0 (myemail@example.com)',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov',
            'Connection': 'keep-alive'
        })

        total_quarters = 0
        for year in range(start_year, end_year + 1):
            available_quarters = check_available_quarters(base_url, year, session)
            total_quarters += len(available_quarters)
        
        overall_progress_bar = tqdm(total=total_quarters, desc="Processing Quarters", unit="quarter")
        
        for year in range(start_year, end_year + 1):
            available_quarters = check_available_quarters(base_url, year, session)
            for quarter in available_quarters:
                transactions = list(fetch_form_idx(base_url, year, quarter, session))
                
                with tqdm(total=len(transactions), desc=f"Processing {year} {quarter}", unit="form", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}] {percentage:3.0f}%") as quarter_progress_bar:
                    for transaction in transactions:
                        transactions_batch.append(transaction)
                        
                        if len(transactions_batch) >= batch_size:
                            total_data_transferred += save_transactions_batch(transactions_batch, base_dir, session, quarter_progress_bar)
                            transactions_batch = []  
                    
                    if transactions_batch:
                        total_data_transferred += save_transactions_batch(transactions_batch, base_dir, session, quarter_progress_bar)
                
                overall_progress_bar.update(1)
        
        overall_progress_bar.close()
        print(f"Total Data Transferred: {total_data_transferred / (1024 * 1024):.2f} MB")

def fetch_form_idx(base_url: str, year: int, quarter: str, session):
    url = f"{base_url}/{year}/{quarter}/form.idx"
    
    response = session.get(url)
    time.sleep(0.2)
    if response.status_code == 200:
        return parse_form_idx_correctly(response.text.splitlines())
    else:
        raise Exception(f"Failed to download {year} {quarter} form.idx, status code: {response.status_code}")

def parse_form_idx_correctly(lines):
    """
    Parses the content of the form.idx file using a generator.
    
    Parameters:
    - lines: An iterable of lines from the form.idx file.

    Yields:
    - A dictionary with relevant transaction information (e.g., CIK, File Name).
    """
    for line in lines:
        if re.match(r'^[ \-]*$', line) or line.startswith('Form Type'):
            continue
        parts = re.split(r'\s{2,}', line.strip())
        if parts[0].strip() == '4':  # Only process Form 4 filings
            yield {
                "form_type": parts[0].strip(),
                "company_name": parts[1].strip(),
                "cik": parts[2].strip(),
                "date_filed": parts[3].strip(),
                "file_name": parts[4].strip()
            }

# Configuration
base_url = "https://www.sec.gov/Archives/edgar/full-index"
base_dir = "./form4_data"
start_year = 2023
end_year = 2024
batch_size = 100

# Process Transactions
process_transactions(base_url, start_year, end_year, base_dir, batch_size)


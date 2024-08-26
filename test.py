import requests
import os
import time
import datetime

# Directory to save downloaded files
local_directory = "./edgar_filings"

if not os.path.exists(local_directory):
    os.makedirs(local_directory)

# Define the base URL for the SEC EDGAR Full Text Search API
base_url = "https://www.sec.gov/Archives/edgar/data"
base_url = "https://www.sec.gov/Archives/edgar/full-index/"
headers = {
    'User-Agent': 'none fys123go@gmail.com',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov',
    'Connection': 'keep-alive'
}

# Get today's date
today = datetime.datetime.today().strftime('%Y%m%d')

# Function to download and save a filing
def download_filing(cik, accession_number):
    url = "https://www.sec.gov/Archives/edgar/data/320193/000032019322000111/000032019322000111-index.html"
    url = "https://www.sec.gov/Archives/edgar/data/1541617/000110465920125814/xslForm13F_X01/infotable.xml"
    print(url)
    response = requests.get(url, headers=headers)
    print(response)
    
    if response.status_code == 200:
        file_name = f"filing_{cik}_{accession_number}.html"
        file_path = os.path.join(local_directory, file_name)
        
        with open(file_path, 'wb') as file:
            file.write(response.content)
        
        print(f"Downloaded filing {accession_number} for CIK {cik} and saved as {file_name}")
    else:
        print(f"Failed to download filing. Status code: {response.status_code}")

# List of example CIKs and accession numbers (you would typically obtain these from the index)
filings = [
    {"cik": "0000320193", "accession_number": "0000320193-22-000111"},
    {"cik": "0001067983", "accession_number": "0001067983-22-000003"},
    # Add more CIKs and accession numbers as needed
]

# Download filings with a delay to avoid overloading the server
for filing in filings:
    download_filing(filing["cik"], filing["accession_number"])
    time.sleep(2)  # 10 seconds delay between requests

print("Download complete.")

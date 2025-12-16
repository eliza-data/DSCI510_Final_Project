import requests
import csv
import os
import time
from typing import List, Dict, Any

# API URL for the "Medicare Part D Prescribers - by Provider and Drug" dataset
BASE_URL: str = "https://data.cms.gov/data-api/v1/dataset/9552739e-3d05-4c1b-8eff-ecabf391e2e5/data"

# API Parameters Restrictions 
API_PAGE_SIZE: int = 5000  # Max number of records to fetch per API call

#MAX_ROWS_FOR_TESTING: int | None = 10000  #testing smaller dataset
MAX_ROWS_FOR_TESTING: int | None = None 

# Data Year to filter 
TARGET_YEAR: int = 2023

# Five target states (Prscrbr_State_Abrvtn column)
STATES: List[str] = [
    "CA", "TX", "FL", "NY", "PA"
]

# The five SSRI Generic Names
GENERIC_NAMES: List[str] = [
    'SERTRALINE HCL',
    'FLUOXETINE HCL',
    'ESCITALOPRAM OXALATE',
    'PAROXETINE HCL',
    'CITALOPRAM HBR'
]

# The five SSRI Brand Names
BRAND_NAMES: List[str] = [
    'ZOLOFT',
    'PROZAC',
    'LEXAPRO',
    'PAXIL', 
    'CELEXA'
]

# Combined list of all 10 drug names (for comprehensive filtering)
ALL_DRUG_NAMES: List[str] = GENERIC_NAMES + BRAND_NAMES

# Output Path
OUTPUT_DIR: str = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw')
OUTPUT_FILENAME: str = "ssri_partd_2023_five_states_raw.csv"
OUTPUT_FILE_PATH: str = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

def _create_multi_condition_filter(columns: List[str], values: List[str], condition_name: str) -> Dict[str, Any]:
  
    filter_params = {}
    i = 0
    # Iterate through every drug name
    for drug_name in values:
        # For each drug name, check for Generic and Brand columns 
        for column in columns:
            filter_key = f'filter[{condition_name}-{i}]'
            
            filter_params[f'{filter_key}[condition][path]'] = column
            filter_params[f'{filter_key}[condition][operator]'] = '==='
            filter_params[f'{filter_key}[condition][value]'] = drug_name
            i += 1
            
    return filter_params

def build_filter_params() -> Dict[str, Any]:
    """
    API query parameters.
    """
    all_filters = {}

    # 1. State Filter (OR)
    # The API combines all state filters with OR logic.
    for i, state in enumerate(STATES):
        filter_key = f'filter[state_filter-{i}]'
        all_filters[f'{filter_key}[condition][path]'] = 'Prscrbr_State_Abrvtn'
        all_filters[f'{filter_key}[condition][operator]'] = '==='
        all_filters[f'{filter_key}[condition][value]'] = state
    
    
    # 2. Drug Filter 
    # (Gnrc_Name=Sertraline OR Brnd_Name=Sertraline) OR (Gnrc_Name=Zoloft OR Brnd_Name=Zoloft) 
   
    drug_filters = _create_multi_condition_filter(
        columns=['Gnrc_Name', 'Brnd_Name'], 
        values=ALL_DRUG_NAMES, 
        condition_name='drug_filter'
    )
    all_filters.update(drug_filters)
    
    # 3. Year Filter 
    # The API automatically ANDs this filter block with the State and Drug filter blocks.
    year_key = 'filter[year_filter]'
    all_filters[f'{year_key}[condition][path]'] = 'Data_Yr'
    all_filters[f'{year_key}[condition][operator]'] = '==='
    all_filters[f'{year_key}[condition][value]'] = str(TARGET_YEAR)

    return all_filters


def fetch_page(offset: int, filter_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch one page of results from CMS API.
    """
    # Temporarily remove 'size' from filter_params and include it in 'params'
    # so that the size logic can be handled by the calling function (fetch_all_data)
    size = filter_params.pop('size', API_PAGE_SIZE) 
    
    params = {
        "size": size,
        "offset": offset
    }
    params.update(filter_params)

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status() 
        
        data = response.json()
        if isinstance(data, list):
            return data
        else:
            print(f"ERROR: Unexpected response format (not a list) at offset {offset}. Response: {data}")
            return []
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: Could not fetch data at offset {offset}. Status code: {e.response.status_code}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"An general error occurred during API request: {e}")
        return []


def fetch_all_data() -> List[Dict[str, Any]]:
    """
    Fetches all data, handling pagination until the entire filtered dataset is retrieved.
    """
    filter_params = build_filter_params()
    all_rows: List[Dict[str, Any]] = []
    offset: int = 0
    total_fetched: int = 0
    
    current_page_size = API_PAGE_SIZE
    
    if MAX_ROWS_FOR_TESTING is not None:
        print(f"MAX_ROWS_FOR_TESTING is set to {MAX_ROWS_FOR_TESTING}. This is NOT the full dataset.")
    
    print(f"Fetching data for 5 States and 5 SSRIs (Generic + Brand) in {TARGET_YEAR}...")

    while True:
        # 1. Determine size for this fetch
        if MAX_ROWS_FOR_TESTING is not None:
            remaining = MAX_ROWS_FOR_TESTING - total_fetched
            if remaining <= 0:
                print(f"MAX_ROWS_FOR_TESTING limit of {MAX_ROWS_FOR_TESTING} reached. Stopping fetch.")
                break
            current_page_size = min(API_PAGE_SIZE, remaining)
        else:
            current_page_size = API_PAGE_SIZE

        # 2. Add size to filter parameters for the current page fetch
        page_filter_params = filter_params.copy()
        page_filter_params['size'] = current_page_size

        print(f"Fetching page at offset {offset} (Total fetched: {total_fetched}, Page size: {current_page_size})...")

        rows = fetch_page(offset, page_filter_params)

        if not rows:
            print("No more data returned or an error occurred. Stopping fetch.")
            break

        # 3. Process and check limits
        all_rows.extend(rows)
        records_on_page = len(rows)
        total_fetched += records_on_page
        
        if records_on_page < current_page_size:
            print(f"Received {records_on_page} records, indicating the end of the filtered dataset.")
            break
        
        if MAX_ROWS_FOR_TESTING is not None and total_fetched >= MAX_ROWS_FOR_TESTING:
            print(f"MAX_ROWS_FOR_TESTING limit of {MAX_ROWS_FOR_TESTING} reached. Stopping fetch.")
            break

        offset += API_PAGE_SIZE
        time.sleep(0.5) 
        
    print(f"\nCollected a TOTAL of {total_fetched} records.")
    return all_rows


def save_to_csv(rows: List[Dict[str, Any]]):
    """
    Saves the list of dictionaries (JSON records) to a CSV file.
    """
    if not rows:
        print("No data to save.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Ensure the rows are not empty before getting keys
    if not rows:
        print("No data to save.")
        return

    # Use the keys from the first row as the field names for the CSV header
    fieldnames = rows[0].keys()

    try:
        with open(OUTPUT_FILE_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Raw data successfully saved to: {OUTPUT_FILE_PATH}")
    except Exception as e:
        print(f"ERROR: Failed to save CSV file: {e}")

"""Main execution function."""

def main():
    print("Starting CMS Part D SSRI Data Collection Script")
    
    rows = fetch_all_data()
    save_to_csv(rows)


if __name__ == "__main__":
    main()

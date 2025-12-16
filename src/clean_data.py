import pandas as pd
import os
from typing import Dict, List

# Define relative paths based on the standard project structure
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..', '..')
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'ssri_partd_2023_five_states_raw.csv')
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed', 'ssri_partd_2023_five_states_aggregated.csv')

# The standardized mapping from all drug names (Generic and Brand) 
# Keys MUST be in consistent format (UPPERCASE and no extra spaces)
DRUG_NAME_MAPPING: Dict[str, str] = {
    # Sertraline
    'SERTRALINE HCL': 'Sertraline (Zoloft)',
    'ZOLOFT': 'Sertraline (Zoloft)',
    
    # Fluoxetine
    'FLUOXETINE HCL': 'Fluoxetine (Prozac)',
    'PROZAC': 'Fluoxetine (Prozac)',
    
    # Escitalopram
    'ESCITALOPRAM OXALATE': 'Escitalopram (Lexapro)',
    'LEXAPRO': 'Escitalopram (Lexapro)',
    
    # Paroxetine
    'PAROXETINE HCL': 'Paroxetine (Paxil)',
    'PAXIL': 'Paroxetine (Paxil)',
    
    # Citalopram
    'CITALOPRAM HBR': 'Citalopram (Celexa)',
    'CELEXA': 'Citalopram (Celexa)',
}

# The columns we need to read from the raw data
REQUIRED_COLUMNS: List[str] = [
    'Gnrc_Name',          # Generic Name
    'Brnd_Name',          # Brand Name
    'Prscrbr_State_Abrvtn', # State Abbreviation
    'Tot_Clms'            # Total Claims (Prescription Count)
]

def clean_and_aggregate_data():
    """
   Data cleaning, standardization, and aggregation 
    """
    print("Starting Data Cleaning and Aggregation ")
    
    # 1. Load Data
    try:
        # Read only the necessary columns
        df = pd.read_csv(RAW_DATA_PATH, usecols=REQUIRED_COLUMNS)
        
        if len(df) == 0:
            print(f"ERROR: Raw data file at {RAW_DATA_PATH} was loaded but contained ZERO records (only headers).")
            print("Please re-run get_data.py to ensure it fetches data successfully.")
            return

        print(f"Successfully loaded {len(df)} raw records.")
    except FileNotFoundError:
        print(f"FATAL ERROR: Raw data file not found at {RAW_DATA_PATH}.")
        print("Please ensure you have successfully run get_data.py first.")
        return
    except Exception as e:
        print(f"FATAL ERROR during data loading: {e}")
        return

    # 2. Standardization & Cleaning (Critical Fix)
    # Standardize all drug and state columns to uppercase and strip whitespace 
    df['Gnrc_Name'] = df['Gnrc_Name'].astype(str).str.strip().str.upper()
    df['Brnd_Name'] = df['Brnd_Name'].astype(str).str.strip().str.upper()
    df['Prscrbr_State_Abrvtn'] = df['Prscrbr_State_Abrvtn'].astype(str).str.strip().str.upper()
    
    # Ensure prescription count column is numeric
    df['Tot_Clms'] = pd.to_numeric(df['Tot_Clms'], errors='coerce').fillna(0).astype(int)
    print("Columns standardized (case and type cleaned).")

    # 3. Group Brand to Generic Equivalents (5.2.b)
    def map_drug_name(row):
        """Looks up the standardized name using the cleaned Generic or Brand name."""
        generic = row['Gnrc_Name']
        brand = row['Brnd_Name']
        
        # We only need to check if the cleaned value is in the mapping dictionary
        if generic in DRUG_NAME_MAPPING:
            return DRUG_NAME_MAPPING[generic]
        elif brand in DRUG_NAME_MAPPING:
            return DRUG_NAME_MAPPING[brand]
        else:
            return None 

    df['Antidepressant_Group'] = df.apply(map_drug_name, axis=1)

    # 4. 
    # Drop rows that did not match the SSRI mapping (Antidepressant_Group is None)
    df_cleaned = df.dropna(subset=['Antidepressant_Group'])
    
    # Filter the state column (should only be 5 target states)
    target_states = ['CA', 'TX', 'FL', 'NY', 'PA']
    df_cleaned = df_cleaned[df_cleaned['Prscrbr_State_Abrvtn'].isin(target_states)]

    print(f"Filtered to {len(df_cleaned)} valid SSRI records across 5 states.")

    if len(df_cleaned) == 0:
        print("ERROR: All data was filtered out! This suggests a mismatch between raw data content and the DRUG_NAME_MAPPING.")
        return

    # 5. Aggregation 
    df_aggregated = df_cleaned.groupby(
        ['Prscrbr_State_Abrvtn', 'Antidepressant_Group']
    )['Tot_Clms'].sum().reset_index()

    # Rename the State column for clarity
    df_aggregated = df_aggregated.rename(columns={'Prscrbr_State_Abrvtn': 'State_Abrvtn'})
    
    print(f"Data successfully aggregated into {len(df_aggregated)} total unique combinations (State/SSRI).")
    
    # 6. Save Processed Data
    os.makedirs(os.path.dirname(PROCESSED_DATA_PATH), exist_ok=True)
    df_aggregated.to_csv(PROCESSED_DATA_PATH, index=False)
    
    print(f"\nSUCCESS: Cleaned and Aggregated data saved to: {PROCESSED_DATA_PATH}")


if __name__ == "__main__":
    clean_and_aggregate_data()

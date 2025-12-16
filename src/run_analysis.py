import pandas as pd
import os
from typing import Tuple

# Determine the absolute path to the base 'project_files' directory.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) 
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, '..')) 
PROJECT_ROOT = os.path.abspath(os.path.join(SRC_DIR, '..')) 

# Locates the aggregated CSV 
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed', 'ssri_partd_2023_five_states_aggregated.csv')


def load_data() -> pd.DataFrame | None:
    """Loads the processed, aggregated data from the data/processed folder."""
    try:
        if not os.path.exists(PROCESSED_DATA_PATH):
             print("\n========================================================")
             print("Aggregated data file not found.")
             print(f"The script looked for the file here: {PROCESSED_DATA_PATH}")
             print("Please ensure clean_data.py ran successfully and created the file.")
             print("========================================================\n")
             return None

        df = pd.read_csv(PROCESSED_DATA_PATH)
        if len(df) == 0:
            print("ERROR: Loaded data is empty. ")
            return None
            
        print(f"Successfully loaded aggregated data with {len(df)} records (Expected ~25).")
        return df
    except Exception as e:
        print(f"An unexpected error occurred during data loading: {e}")
        return None

def perform_analysis(df: pd.DataFrame):
    """
    Performs the required statistical ranking and prints the findings for the final report.
    (This fulfills Deliverable 5.3.b and 5.3.c).
    """
    print("--- DATA ANALYSIS FINDINGS (for Final Report) ---")
    

    # A. Overall State Ranking (Total Prescriptions) 
    state_ranking = df.groupby('State_Abrvtn')['Tot_Clms'].sum().sort_values(ascending=False).reset_index()
    state_ranking = state_ranking.rename(columns={'Tot_Clms': 'Total_SSRI_Claims'})
    state_ranking['Rank'] = state_ranking['Total_SSRI_Claims'].rank(method='dense', ascending=False).astype(int)

    print("\n[FINDING 1: OVERALL STATE RANKING]")
    print(state_ranking.to_string(index=False))
    
    # B. Overall SSRI Ranking (Market Share) 
    ssri_ranking = df.groupby('Antidepressant_Group')['Tot_Clms'].sum().sort_values(ascending=False).reset_index()
    ssri_ranking = ssri_ranking.rename(columns={'Tot_Clms': 'Total_SSRI_Claims'})

    print("\n[FINDING 2: OVERALL SSRI MARKET SHARE]")
    print(ssri_ranking.to_string(index=False))
    
    # C. State Ranking for EACH SSRI (The Core Answer Preparation) 
    # pivot table to compare state claims for each drug
    pivot_df = df.pivot(index='State_Abrvtn', columns='Antidepressant_Group', values='Tot_Clms')
    highest_prescribing_state_per_drug = pivot_df.idxmax(axis=0)
    
    print("\n[FINDING 3: HIGHEST PRESCRIBING STATE PER DRUG]")
    print("This directly answers the research question (Row = SSRI, Value = State).")
    print(highest_prescribing_state_per_drug.rename('Highest_Prescribing_State'))
    
    print("\n--- Analysis Complete ---")


def main():
    df = load_data()
    if df is not None:
        perform_analysis(df)


if __name__ == "__main__":
    main()

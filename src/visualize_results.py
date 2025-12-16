import pandas as pd
import matplotlib.pyplot as plt
import os

# Determine the file path to 'project_files' directory.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) 
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..')) 

# Locates the aggregated CSV 
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed', 'ssri_partd_2023_five_states_aggregated.csv')

# Targets the existing folder for saving figures
FIGURES_DIR = os.path.join(PROJECT_ROOT, 'results', 'visuals')

# plotting style for visualizations
plt.style.use('seaborn-v0_8-whitegrid')


def load_data_and_prepare_viz() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    """Loads processed data and prepares necessary dataframes for visualization."""
    try:
        df = pd.read_csv(PROCESSED_DATA_PATH)
        
        # Prepare dataframes used in the visuals
        state_ranking = df.groupby('State_Abrvtn')['Tot_Clms'].sum().sort_values(ascending=False).reset_index()
        state_ranking = state_ranking.rename(columns={'Tot_Clms': 'Total_SSRI_Claims'})
        
        ssri_ranking_viz = df.groupby('Antidepressant_Group')['Tot_Clms'].sum().sort_values(ascending=True).reset_index()
        
        pivot_df = df.pivot(index='State_Abrvtn', columns='Antidepressant_Group', values='Tot_Clms')
        
        return state_ranking, ssri_ranking_viz, pivot_df
        
    except FileNotFoundError:
        print(f"FATAL ERROR: Cannot find aggregated data at {PROCESSED_DATA_PATH}. Please run clean_data.py first.")
        return None

def create_visualizations(state_ranking: pd.DataFrame, ssri_ranking_viz: pd.DataFrame, pivot_df: pd.DataFrame):
    """
    Generates three key visualizations 
    """
    print("\n--- Generating Visualizations ---")
    
    # Ensure the target directory exists 
    os.makedirs(FIGURES_DIR, exist_ok=True)
    
    # VISUAL 1: Vertical Bar Chart of State Ranking 
   
    plt.figure(figsize=(9, 6))
    claims_in_millions = state_ranking['Total_SSRI_Claims'] / 1e6 
    
    plt.bar(state_ranking['State_Abrvtn'], claims_in_millions, color='darkblue')
    
    plt.title('Figure 1: Total SSRI Prescriptions by State (2023)', fontsize=16)
    plt.xlabel('State Abbreviation', fontsize=12)
    plt.ylabel('Total Claims (Millions)', fontsize=12)
    
    for i, v in enumerate(claims_in_millions):
        plt.text(i, v + 0.5, f'{v:.1f}M', ha='center', va='bottom', fontsize=10)
        
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    fig_path_1 = os.path.join(FIGURES_DIR, '01_state_ranking_total_ssri.png')
    plt.savefig(fig_path_1)
    print(f"Saved Figure 1 (Total State Ranking) to: {fig_path_1}")
    plt.close()

    # VISUAL 2: Stacked Bar Chart - State breakdown of ALL SSRIs 
    plt.figure(figsize=(14, 8))
    
    pivot_in_millions = pivot_df / 1e6
    pivot_in_millions.plot(kind='bar', stacked=True, ax=plt.gca(), cmap='viridis') 
    
    plt.title('Figure 2: Total SSRI Claims Breakdown by State (2023)', fontsize=16)
    plt.xlabel('State Abbreviation', fontsize=12)
    plt.ylabel('Total Claims (Millions)', fontsize=12)
    plt.xticks(rotation=0) 
    
    legend_labels = pivot_in_millions.columns.str.replace(r' \(.*\)', '', regex=True)
    plt.legend(legend_labels, title='SSRI Drug Name', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout(rect=[0, 0, 0.85, 1]) 
    fig_path_2 = os.path.join(FIGURES_DIR, '02_stacked_ssri_breakdown_by_state.png')
    plt.savefig(fig_path_2)
    print(f"Saved Figure 2 (Core Answer - Stacked Breakdown) to: {fig_path_2}")
    plt.close()

    # VISUAL 3: Horizontal Bar Chart of Overall SSRI Market Share

    ssri_claims_millions = ssri_ranking_viz['Tot_Clms'] / 1e6
    
    plt.figure(figsize=(10, 7))
    plt.barh(ssri_ranking_viz['Antidepressant_Group'], ssri_claims_millions, color='teal')
    
    plt.title('Figure 3: Overall Market Share of 5 SSRIs (Total Claims)', fontsize=16)
    plt.xlabel('Total Claims (Millions)', fontsize=12)
    plt.ylabel('Antidepressant Group', fontsize=12)
    
    for i, v in enumerate(ssri_claims_millions):
        plt.text(v + 0.1, i, f'{v:.1f}M', va='center', fontsize=10)
        
    plt.grid(axis='x', linestyle='--', alpha=0.6)
    plt.tight_layout()
    fig_path_3 = os.path.join(FIGURES_DIR, '03_overall_ssri_market_share.png')
    plt.savefig(fig_path_3)
    print(f"Saved Figure 3 (Overall Drug Market Share) to: {fig_path_3}")
    plt.close()


def main():
    prepared_data = load_data_and_prepare_viz()
    if prepared_data is not None:
        state_ranking, ssri_ranking_viz, pivot_df = prepared_data
        create_visualizations(state_ranking, ssri_ranking_viz, pivot_df)


if __name__ == "__main__":
    main()

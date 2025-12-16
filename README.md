# DSCI510_Final_Project
# Project Title: Analysis of SSRI Prescribing Trends in the U.S. (2023)

This project analyzes the prescribing trends of five common Selective Serotonin Reuptake Inhibitors (SSRIs) across the five most populated  U.S. states using public data from the Centers for Medicare and Medicaid Services (CMS) Medicare Part D Prescribers API (2023).

A. Installation and Requirements

To run this project locally, requires Python 3.8 installed. All necessary libraries are listed in the `requirements.txt` file.

1.  Clone the Repository:
    ```bash
    git clone [YOUR_REPOSITORY_URL]
    cd [YOUR_REPOSITORY_FOLDER]
    ```
2.  Install Dependencies:
    Use `pip` to install all required libraries (`pandas`, `requests`, `matplotlib`, etc.).
    ```bash
    pip install -r requirements.txt
    ```

B. Run Code 

The project is executed in a sequential pipeline. Scripts must be run in the order listed below, as each script depends on the output file of the previous script.

1. Data Acquisition (`get_data.py`)

  This script handles the connection to the CMS API to retrieve the raw prescription data.

  Process: The script makes multiple requests to the Medicare Part D API, querying for both generic and brand names of the five target SSRIs     across the five most populous states.
  
  Command:
    ```bash
    python src/get_data.py
    ```
  Output: The script fetches over 26 million records and saves the raw data to the file:
    `data/raw/ssri_partd_2023_five_states_raw.csv`

2. Data Cleaning and Preparation (`clean_data.py`)

  This script processes the massive raw file into a clean, aggregated format suitable for analysis.

  Process:
    - Loads the raw data.
    - Standardizes drug names (generic and brand) and state abbreviations to ensure consistency.
    - Groups all brand names (e.g., Zoloft) to their generic equivalent (e.g., Sertraline).
    - Aggregates the data by State Abbreviation (`State_Abrvtn`) and standardized Drug Group (`Antidepressant_Group`), summing the Total             Claims (`Tot_Clms`).
  Command:
    ```bash
    python src/clean_data.py
    ```
  Output:
  The script reduces the data to a 25-row summary table (5 states x 5 drugs) and saves it to:
    `data/processed/ssri_partd_2023_five_states_aggregated.csv`

3. Running Analysis (`run_analysis.py`)

  This script performs the core statistical calculations and derives the key findings.

  Process:
    - Loads the processed 25-row aggregated data.
    - Uses `pandas` methods (`groupby`, `sum`, `rank`) to calculate:
        - Overall state ranking by total SSRI claims.
        - Overall market share for each SSRI.
        - The single highest prescribing state for each individual SSRI.
  Command:
    ```bash
    python src/run_analysis.py
    ```
  Output:
  The key findings tables are printed for review and use in the final report.

4. Producing Visualizations (`visualize_results.py`)

  This script generates the three visualizations for the final report.

  Process:
    - Loads the processed data.
    - Uses the `matplotlib` library to create three distinct charts:
        - Total State Ranking (Bar Chart)
        - SSRI Claims Breakdown by State (Stacked Bar Chart)
        - Overall SSRI Market Share (Horizontal Bar Chart)
  Command:
    ```bash
    python src/visualize_results.py
    ```
  Output: The three generated charts are saved as PNG files in the visualization directory:
    `results/visuals/`

  

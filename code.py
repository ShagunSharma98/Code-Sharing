import awswrangler as wr # For Athena and S3 interaction with Pandas
import pandas as pd
import numpy as np
import re # For regex used in compress equivalent

# --- Configuration ---
ATHENA_DATABASE = 'your_athena_database' # Your main Athena database
S3_STAGING_DIR = 's3://your-data-bucket/python_processing_staging/' # For intermediate/final DataFrames

# Macro variable equivalents (Python variables)
archive_list_py = ['BO_EXAMPLE_ARCHIVED1', 'BO_EXAMPLE_ARCHIVED2'] # Example
per_py = '202306' # Example YYYYMM, campaign processing cutoff period

# --- Helper Function to Read Data from Athena ---
def read_table_from_athena(table_name, database=ATHENA_DATABASE, columns=None):
    """Reads specified columns from an Athena table into a Pandas DataFrame."""
    col_str = "*" if columns is None else ", ".join(columns)
    sql_query = f"SELECT {col_str} FROM {database}.{table_name}"
    print(f"Reading from Athena: {table_name} ({'all' if columns is None else len(columns)} cols)...")
    try:
        df = wr.athena.read_sql_query(sql=sql_query, database=database, ctas_approach=False) # Ensure no CTAS
        print(f"Read {len(df)} rows from {table_name}.")
    except Exception as e:
        print(f"Error reading {table_name}: {e}. Returning empty DataFrame.")
        # Return empty dataframe with expected columns if possible, or raise
        if columns:
            df = pd.DataFrame(columns=columns)
        else: # Cannot infer columns if SELECT * failed on non-existent table
            raise e # Or handle more gracefully
    return df

# --- Helper Functions to Save/Load DataFrames to/from S3 (Parquet is efficient) ---
def save_df_to_s3(df, s3_path_key, description="DataFrame"):
    """Saves a Pandas DataFrame to S3 as Parquet."""
    s3_full_path = f"{S3_STAGING_DIR}{s3_path_key}.parquet"
    print(f"Saving {description} to {s3_full_path} ({len(df)} rows)...")
    wr.s3.to_parquet(df=df, path=s3_full_path, index=False)
    print(f"Saved {description} to {s3_full_path}")
    return s3_full_path

def load_df_from_s3(s3_path_key, description="DataFrame"):
    """Loads a Pandas DataFrame from S3 Parquet."""
    s3_full_path = f"{S3_STAGING_DIR}{s3_path_key}.parquet"
    print(f"Loading {description} from {s3_full_path}...")
    df = wr.s3.read_parquet(path=s3_full_path)
    print(f"Loaded {description} from {s3_full_path} ({len(df)} rows).")
    return df

# --- SAS Function Equivalents in Python ---
def sas_compress(series_or_string):
    """Equivalent to SAS compress() function for removing all whitespace."""
    if isinstance(series_or_string, pd.Series):
        return series_or_string.astype(str).str.replace(r'\s+', '', regex=True)
    elif isinstance(series_or_string, str):
        return re.sub(r'\s+', '', series_or_string)
    return series_or_string

def sas_upcase(series_or_string):
    """Equivalent to SAS UPCASE() function."""
    if isinstance(series_or_string, pd.Series):
        return series_or_string.astype(str).str.upper()
    elif isinstance(series_or_string, str):
        return series_or_string.upper()
    return series_or_string

# --- Main Script Logic ---

def part1_data_preparation(archive_list, per_cutoff_str):
    print("\n--- Part 1: Data Preparation (Python/Pandas) ---")

    # 1. Create cld_master_df (from cld.contact_log_master)
    # Define needed columns to reduce memory footprint early
    cld_contact_log_cols = ['Campaign_id', 'MARKETING_CELL', 'acct_id', 'TEST_CONTROL', 'PD_19', 'MOB', 'Period', 'CPC', 'BFRCD', 'REGION', 'DM_ELIG', 'EM_ELIG', 'fico', 'fulfilled_rewards', 'Campaign_Description'] # Add all used cols
    cld_contact_log_df = read_table_from_athena('cld_contact_log_master', columns=cld_contact_log_cols)
    
    cld_master_df = cld_contact_log_df[~cld_contact_log_df['Campaign_id'].isin(archive_list)].copy()

    # Hardcode Opt_in status
    conditions_opt_in = [ # Fill these based on your SAS code's B0... campaign IDs
        (sas_compress(cld_master_df['Campaign_id']) == 'B031836'), (sas_compress(cld_master_df['Campaign_id']) == 'B038887'),
        (sas_compress(cld_master_df['Campaign_id']) == 'B052180') # Example end
    ]
    choices_opt_in = ['N', 'Y', ..., 'N'] # Match conditions
    cld_master_df['Opt_in_hardcoded'] = np.select(conditions_opt_in, choices_opt_in, default=None)

    # 2. Collect Distinct Campaign IDs for opt_in filtering
    distinct_campaign_ids_for_optin = cld_contact_log_df['Campaign_id'].unique()
    
    # 3. Process Opt-in Account Data (optin.optin_accts)
    optin_accts_cols = ['CMPGN_ID', 'acct_id', 'cell', 'CMNCN_LOG_ID', 'accept_dt', 'opt_acct'] # Add all used cols
    optin_accts_df = read_table_from_athena('optin_optin_accts', columns=optin_accts_cols)
    
    opt_in_df = optin_accts_df[optin_accts_df['CMPGN_ID'].isin(distinct_campaign_ids_for_optin)].copy()
    opt_in_df['accept_dt_pd'] = pd.to_datetime(opt_in_df['accept_dt'], errors='coerce')
    opt_in_df['accept_month_optin'] = opt_in_df['accept_dt_pd'].dt.strftime('%Y%m')
    opt_in_df['accept_date_optin'] = opt_in_df['accept_dt_pd'] # Keep as datetime for now
    opt_in_df['mktg_cell_optin'] = pd.to_numeric(opt_in_df['cell'], errors='coerce')
    # Keep only necessary columns for merge + data
    opt_in_df = opt_in_df[['CMPGN_ID', 'acct_id', 'mktg_cell_optin', 'CMNCN_LOG_ID', 'accept_month_optin', 'opt_acct']].copy()


    # 4. Join cld_master_df with opt_in_df -> cm_cld_master_v1_df
    cm_cld_master_v1_df = pd.merge(
        cld_master_df, opt_in_df,
        left_on=['Campaign_id', 'MARKETING_CELL', 'acct_id'],
        right_on=['CMPGN_ID', 'mktg_cell_optin', 'acct_id'],
        how='left', suffixes=('', '_oin') # _oin for opt_in merge
    )
    
    # 5. Further enrich cm_cld_master_v1_df -> cm_cld_master_v2_df
    cm_cld_master_v2_df = cm_cld_master_v1_df.copy()
    # Apply SAS formats using dictionary mappings
    # Example: origin_format_map = {'CODE1': 'Label1', ...}
    # cm_cld_master_v2_df['ORIGIN'] = cm_cld_master_v2_df['CPC'].map(origin_format_map)
    # ... PLATFORM, PRODUCT, SEGMENT, PARTNER ...

    # Opt-in Logic Refinement
    is_opt_in_N = cm_cld_master_v2_df['Opt_in_hardcoded'] == 'N'
    cols_to_null_if_optin_N = { # target_col: source_col_from_optin_merge (if different)
        'CMNCN_LOG_ID_final': 'CMNCN_LOG_ID_oin', 
        'CMPGN_ID_final': 'CMPGN_ID_oin',
        'accept_month_final': 'accept_month_optin_oin', # Note: accept_month_optin was from opt_in_df
        'opt_acct_final': 'opt_acct_oin',
        'mktg_cell_final': 'mktg_cell_optin_oin'
    }
    for target_col, src_col in cols_to_null_if_optin_N.items():
        cm_cld_master_v2_df[target_col] = cm_cld_master_v2_df[src_col] # Initialize
        if 'mktg_cell' in target_col:
            cm_cld_master_v2_df.loc[is_opt_in_N, target_col] = np.nan
        else:
            cm_cld_master_v2_df.loc[is_opt_in_N, target_col] = '' # Or np.nan for non-string

    # Test/Control Logic for 'optin_status_derived'
    conditions_optin_derived = [
        (sas_upcase(cm_cld_master_v2_df['TEST_CONTROL']) == 'CONTROL'),
        (sas_upcase(cm_cld_master_v2_df['TEST_CONTROL']) == 'TEST') & (cm_cld_master_v2_df['opt_acct_final'] == 1),
        (sas_upcase(cm_cld_master_v2_df['TEST_CONTROL']) == 'TEST') & ((cm_cld_master_v2_df['opt_acct_final'] != 1) | (cm_cld_master_v2_df['opt_acct_final'].isnull()))
    ]
    choices_optin_derived = ['NA', 'Y', 'N']
    cm_cld_master_v2_df['optin_status_derived'] = np.select(conditions_optin_derived, choices_optin_derived, default=None)
    
    cm_cld_master_v2_df['pd19_calculated'] = pd.to_numeric(cm_cld_master_v2_df['PD_19'], errors='coerce') * 100
    
    # Binning for MOB_range, fico_range, pd19_range (example for MOB)
    mob_conditions = [ cm_cld_master_v2_df['MOB'].isnull(), cm_cld_master_v2_df['MOB'] < 3, ...]
    mob_choices = ['I.Missing', 'A.<3', ...]
    cm_cld_master_v2_df['MOB_range'] = np.select(mob_conditions, mob_choices, default='H.60+')
    # ... similarly for fico_range (on 'fico' column) and pd19_range (on 'pd19_calculated')

    save_df_to_s3(cm_cld_master_v2_df, "cm_cld_master_v2_df", "Pre-Time Expansion Data")
    return "cm_cld_master_v2_df" # Return key for loading

def part1_time_expansion_and_asp_join(cm_cld_master_v2_key, per_cutoff_str):
    cm_cld_master_v2_df = load_df_from_s3(cm_cld_master_v2_key, "Pre-Time Expansion Data")

    # 6. Time Series Expansion -> base_master_df
    cm_cld_master_v2_df['Period_numeric'] = pd.to_numeric(cm_cld_master_v2_df['Period'], errors='coerce')
    per_cutoff_numeric = pd.to_numeric(per_cutoff_str, errors='coerce')
    
    df_for_expansion = cm_cld_master_v2_df[cm_cld_master_v2_df['Period_numeric'] <= per_cutoff_numeric].copy()
    
    t_values = pd.DataFrame({'t_value': range(-3, 12)}) # -3 to 11 inclusive
    
    base_master_df = pd.merge(df_for_expansion.assign(_cross_key=1), t_values.assign(_cross_key=1), on='_cross_key', how='outer').drop('_cross_key', axis=1)
    
    base_master_df['Period_dt'] = pd.to_datetime(base_master_df['Period'].astype(str) + '01', format='%Y%m%d', errors='coerce')
    base_master_df['Period1_dt'] = base_master_df.apply(
        lambda row: row['Period_dt'] + pd.DateOffset(months=int(row['t_value'])) if pd.notnull(row['Period_dt']) else pd.NaT, axis=1
    )
    base_master_df['Period1'] = base_master_df['Period1_dt'].dt.strftime('%Y%m') # YYYYMM format
    
    # 7. Join with ASP Master Data (aspmstr.asp_master) -> main1_base_master_v1_df
    asp_master_cols = ['acct_id', 'Period1', ...] # Add all columns needed from asp_master, ensure Period1 matches format
    asp_master_df = read_table_from_athena('aspmstr_asp_master', columns=asp_master_cols)
    # Ensure asp_master_df.Period1 is also string 'YYYYMM'
    # asp_master_df['Period1'] = asp_master_df['SomePeriodCol'].dt.strftime('%Y%m') # if conversion needed

    main1_base_master_v1_df = pd.merge(
        base_master_df, asp_master_df,
        on=['acct_id', 'Period1'], how='left', suffixes=('', '_asp')
    )
    save_df_to_s3(main1_base_master_v1_df, "main1_base_master_v1_df", "Base Master with ASP")
    return "main1_base_master_v1_df"


def part2_financial_enrichment_and_joins(main1_base_master_v1_key):
    print("\n--- Part 2: Financial Enrichment and Further Joins (Python/Pandas) ---")
    main1_base_master_v1_df = load_df_from_s3(main1_base_master_v1_key, "Base Master with ASP")
    base_master_v2_df = main1_base_master_v1_df.copy()

    # 8. Data Enrichment - Financials
    # Flag Conversion (ACCT_OPEN_NUM, etc.)
    flag_cols_sas = ['ACCT_OPEN', 'ACCT_ACTV', ...] # List all flag columns from SAS
    for col_sas in flag_cols_sas:
        # Determine actual column name (might have _asp suffix if it came from asp_master)
        actual_col = col_sas if col_sas in base_master_v2_df else f"{col_sas}_asp"
        if actual_col not in base_master_v2_df:
            print(f"Warning: Flag source {actual_col} for {col_sas}_NUM not found. Defaulting to 0.")
            base_master_v2_df[f"{col_sas}_NUM"] = 0
            continue
        base_master_v2_df[f"{col_sas}_NUM"] = np.where(base_master_v2_df[actual_col] == 'Y', 1, 0)

    # Bronco_Flag
    asset_col = 'ASSET_CLASS_CD' if 'ASSET_CLASS_CD' in base_master_v2_df else 'ASSET_CLASS_CD_asp'
    if asset_col in base_master_v2_df:
      base_master_v2_df['Bronco_Flag'] = np.where(base_master_v2_df[asset_col] != 'Sold', 'NON-BRONCO', 'BRONCO')
    else:
      print("Warning: ASSET_CLASS_CD not found for Bronco_Flag.")
      base_master_v2_df['Bronco_Flag'] = None

    # Financial Calculations (ensure source columns are numeric and handle NaNs)
    # Example: base_master_v2_df['CHRGBOFF_AMT_num'] = pd.to_numeric(base_master_v2_df['CHRGBOFF_AMT_asp'], errors='coerce').fillna(0)
    # ... convert ALL financial source columns to numeric, handling _asp suffixes ...
    # Then perform calculations like:
    # base_master_v2_df['cr_loss_net_amt'] = base_master_v2_df['CHRGBOFF_AMT_num'] + base_master_v2_df['RECOVRY_ACTL_TOT_AMT_num'] * -1
    # ... ALL financial calculations from SAS ...

    # Delinquency Flags (DQ_p_unit, etc.)
    # base_master_v2_df['DQ_p_unit'] = np.where((base_master_v2_df['CO_ALL_IND_NUM'] == 0) & ..., 1, 0)
    # ... ALL delinquency flag calculations ...

    # 9. Prepare MCC Data (mcc_master_df from aspmstr.mcc_master)
    mcc_cols = ['acct_id', 'Period', 'amt_GAFINC', 'cnt_GAFINC', 'Points_GAFINC', 'net_trans_amt', 'net_trans_cnt', 'Points', ...] # And spend category columns like amt_AIRLINES etc.
    mcc_master_df = read_table_from_athena('aspmstr_mcc_master', columns=mcc_cols)
    mcc_master_df['Period1_mcc'] = pd.to_datetime(mcc_master_df['Period'], errors='coerce').dt.strftime('%Y%m')
    mcc_numeric_cols = ['amt_GAFINC', 'cnt_GAFINC', 'Points_GAFINC', 'net_trans_amt', 'net_trans_cnt', 'Points']
    for col in mcc_numeric_cols: mcc_master_df[col] = pd.to_numeric(mcc_master_df[col], errors='coerce').fillna(0)
    mcc_master_df['amt_NON_GAFINC'] = mcc_master_df['net_trans_amt'] - mcc_master_df['amt_GAFINC']
    # ... cnt_NON_GAFINC, Points_NON_GAFINC ...

    # 10. Final Major Joins -> main1_base_master_v3_df
    fulfillment_cols = ['Campaign_id', 'acct_id', 'Period1', ...] # Columns from fl.fulfillment
    fulfillment_df = read_table_from_athena('fl_fulfillment', columns=fulfillment_cols)
    # Ensure fulfillment_df.Period1 is string 'YYYYMM' for merging

    rwa_cols = ['acct_id', 'Period1', ...] # Columns from rds.RWA
    rwa_df = read_table_from_athena('rds_RWA', columns=rwa_cols)
    # Ensure rwa_df.Period1 is string 'YYYYMM'

    temp_df = pd.merge(base_master_v2_df, fulfillment_df, on=['Campaign_id', 'acct_id', 'Period1'], how='left', suffixes=('', '_fulfill'))
    temp_df = pd.merge(temp_df, mcc_master_df, left_on=['acct_id', 'Period1'], right_on=['acct_id', 'Period1_mcc'], how='left', suffixes=('', '_mcc'))
    main1_base_master_v3_df = pd.merge(temp_df, rwa_df, on=['acct_id', 'Period1'], how='left', suffixes=('', '_rwa'))
    
    save_df_to_s3(main1_base_master_v3_df, "main1_base_master_v3_df", "Fully Enriched Base Data")
    return "main1_base_master_v3_df"

def part3_summarization_and_export(main1_base_master_v3_key, archive_list):
    print("\n--- Part 3: Summarization and Export (Python/Pandas) ---")
    main1_base_master_v3_df = load_df_from_s3(main1_base_master_v3_key, "Fully Enriched Base Data")

    # 11. Create Summary Table (main_tc_summ_df)
    # Define grouping columns (ensure they exist after merges and renames)
    group_by_cols = ['CAMPAIGN_ID', 'Campaign_Description', 'MARKETING_CELL', 'TEST_CONTROL', # Use final TEST_CONTROL
                     'BFRCD', # offer
                     'REGION', 'CPC', 'DM_ELIG', 'EM_ELIG', 
                     'optin_status_derived', # Optin
                     'accept_month_final', # accept_month
                     'Bronco_Flag', 'Period1', # Period
                     't_value' # t
                    ] 
    # Check if all group_by_cols exist, handle missing ones or correct names
    existing_group_by_cols = [col for col in group_by_cols if col in main1_base_master_v3_df.columns]
    if len(existing_group_by_cols) != len(group_by_cols):
        print(f"Warning: Not all grouping columns found. Using: {existing_group_by_cols}")

    # Define aggregations (ensure aggregated columns are numeric)
    # Example: main1_base_master_v3_df['opt_acct_final_num'] = pd.to_numeric(main1_base_master_v3_df['opt_acct_final'], errors='coerce').fillna(0)
    agg_operations = {
        'acct_id': 'count', # Accounts
        # 'opt_acct_final_num': 'sum', # opt_count
        # ... ALL other aggregations from SAS (sum of fico, pd19_calculated, all NUM flags, all calculated financials, all _mcc amounts/counts)
    }
    # For distinct count like "fulfill":
    # temp_fulfill = main1_base_master_v3_df[main1_base_master_v3_df['fulfilled_rewards_fulfill'] > 0] # Assuming 'fulfilled_rewards_fulfill'
    # fulfill_counts = temp_fulfill.groupby(existing_group_by_cols)['acct_id'].nunique().reset_index(name='fulfill')
    
    # Ensure all columns for aggregation exist and are numeric
    for col in agg_operations.keys():
        if col not in main1_base_master_v3_df.columns:
            print(f"Warning: Column {col} for aggregation not found. It will be skipped.")
            # agg_operations.pop(col) # Python 3.8+ for dict changes during iteration
            continue
        if not pd.api.types.is_numeric_dtype(main1_base_master_v3_df[col]):
             main1_base_master_v3_df[col] = pd.to_numeric(main1_base_master_v3_df[col], errors='coerce').fillna(0)


    main_tc_summ_df = main1_base_master_v3_df.groupby(existing_group_by_cols, as_index=False).agg(agg_operations)
    # main_tc_summ_df = pd.merge(main_tc_summ_df, fulfill_counts, on=existing_group_by_cols, how='left') # If fulfill_counts done separately
    
    main_tc_summ_df['cell_name'] = 'ALL'
    main_tc_summ_df['View'] = 'Test vs Control'
    # Rename columns to match SAS aliases if needed (e.g., BFRCD to offer)

    # 12. Optin-MatchPair Processing
    mp_master_cols = ['campaign_id', 'opt_in_acct_id', 'mp_acct_id', 'TEST_CONTROL', ...] # Cols from mp.matchpair_master
    mp_master_df = read_table_from_athena('mp_matchpair_master', columns=mp_master_cols)
    
    optin_base_v1_df = pd.merge(mp_master_df, main1_base_master_v3_df,
                                left_on=['campaign_id', 'opt_in_acct_id'],
                                right_on=['CAMPAIGN_ID', 'acct_id'], # CAMPAIGN_ID from v3
                                how='left', suffixes=('_mp', '_base'))
    mp_base_v1_df = pd.merge(mp_master_df, main1_base_master_v3_df,
                             left_on=['campaign_id', 'mp_acct_id'],
                             right_on=['CAMPAIGN_ID', 'acct_id'],
                             how='left', suffixes=('_mp', '_base'))
    main_optin_mp_base_v1_df = pd.concat([optin_base_v1_df, mp_base_v1_df], ignore_index=True)
    
    tc_col_mp = 'TEST_CONTROL_mp' if 'TEST_CONTROL_mp' in main_optin_mp_base_v1_df else 'TEST_CONTROL' # Check source of TC for this df
    conditions_tc_mp = [
        (sas_upcase(main_optin_mp_base_v1_df[tc_col_mp]) == 'TEST'),
        (sas_upcase(main_optin_mp_base_v1_df[tc_col_mp]) == 'CONTROL')
    ]
    main_optin_mp_base_v1_df['TEST_CONTROL_final_mp'] = np.select(conditions_tc_mp, ['Optin', 'MP'], default=main_optin_mp_base_v1_df[tc_col_mp])
    # This df (main_optin_mp_base_v1_df) would then need to be summarized similarly to main_tc_summ_df if SAS's main.optin_mp_summ was a summary.

    # 13. Combine Summaries: main_tc_op_summ_df
    # This step is problematic in SAS as main.optin_mp_summ is not created.
    # Assuming for now that main_tc_op_summ_df is just main_tc_summ_df.
    main_tc_op_summ_df = main_tc_summ_df.copy()
    print("WARNING: main.optin_mp_summ logic from SAS is unclear. main_tc_op_summ_df currently equals main_tc_summ_df.")

    # 14. First Export
    s3_export_path1 = f"{S3_STAGING_DIR}Partner_Spend_Performance.csv"
    save_df_to_s3(main_tc_op_summ_df, "Partner_Spend_Performance_csv_export", "Final Summary (main_tc_op_summ)") # Saved as parquet by helper, then can be converted/read

    # 15. K-Studio Processing
    # Backup existing (conceptual, as we're writing to S3)
    # Split main_tc_op_summ_df
    tc_summ1_df = main_tc_op_summ_df[~main_tc_op_summ_df['CAMPAIGN_ID'].isin(archive_list)].copy()
    # main_check_df = main_tc_op_summ_df[main_tc_op_summ_df['CAMPAIGN_ID'].isin(archive_list)].copy() # If needed

    # SAS %put &tc_summ2, %frq - problematic. Assuming ks output based on tc_summ1_df
    ks_partner_spend_perf_summ_df = tc_summ1_df.copy()

    # 16. Second Export (K-Studio)
    s3_export_path2 = f"{S3_STAGING_DIR}KStudio_Partner_Performance.csv" # SAS had "Perfomance" typo
    save_df_to_s3(ks_partner_spend_perf_summ_df, "KStudio_Partner_Performance_csv_export", "K-Studio Summary")

    print("--- Part 3: Summarization and Export Completed ---")
    return "Partner_Spend_Performance_csv_export", "KStudio_Partner_Performance_csv_export" # Return keys

# --- Main Execution ---
if __name__ == '__main__':
    try:
        # Part 1 (Initial Data + Time Expansion + ASP Join)
        cm_cld_master_v2_key = part1_data_preparation(archive_list_py, per_py)
        main1_base_master_v1_key = part1_time_expansion_and_asp_join(cm_cld_master_v2_key, per_py)
        
        # Part 2 (Financial Enrichment + More Joins)
        main1_base_master_v3_key = part2_financial_enrichment_and_joins(main1_base_master_v1_key)
        
        # Part 3 (Summarization and Export)
        final_summary_key, ks_summary_key = part3_summarization_and_export(main1_base_master_v3_key, archive_list_py)
        
        print(f"\nPython script execution finished. Results saved to S3 under keys: {final_summary_key}, {ks_summary_key}")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
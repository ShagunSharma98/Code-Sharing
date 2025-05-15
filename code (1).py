import pandas as pd
import numpy as np
# Assuming awswrangler as wr and S3 helper functions (save_df_to_s3, load_df_from_s3) are defined elsewhere

def part2_financial_enrichment_and_joins(main1_base_master_v1_key):
    print("\n--- Part 2: Financial Enrichment and Further Joins (Python/Pandas) ---")
    main1_base_master_v1_df = load_df_from_s3(main1_base_master_v1_key, "Base Master with ASP")
    base_master_v2_df = main1_base_master_v1_df.copy() # Work on a copy

    # Define common suffixes used in your merges, in order of likelihood or specificity
    # Customize this list based on your actual merge strategy in part1
    possible_suffixes = ['', '_oin', '_asp', '_mp', '_base', '_fulfill', '_mcc', '_rwa']

    # --- Start: Detailed logic from SAS image ---

    # 1. SAS Array and Loop for Flag Conversion (do j = 1 to 8)
    # Based on SAS arrays:
    # array flag(8) ACCT_OPEN ACCT_ACTV ACTIVE_WITH_BAL FLAG_PURCH ACTV_FLAG REVOLVER CO_ALL_IND CO_MNTH_IND;
    # array char_to_num(8) ACCT_OPEN_NUM ACCT_ACTV_NUM ACTIVE_WITH_BAL_NUM FLAG_PURCH_NUM ACTV_FLAG_NUM REVOLVER_NUM CO_ALL_IND_NUM CO_MNTH_IND_NUM;
    # (The SAS code also listed FRAUD_IND and FRAUD_IND_NUM as the 9th elements,
    #  but the "do j = 1 to 8" loop means only the first 8 are processed here)

    sas_flag_array_elements = [
        'ACCT_OPEN', 'ACCT_ACTV', 'ACTIVE_WITH_BAL', 'FLAG_PURCH',
        'ACTV_FLAG', 'REVOLVER', 'CO_ALL_IND', 'CO_MNTH_IND'
    ]
    sas_char_to_num_array_elements = [
        'ACCT_OPEN_NUM', 'ACCT_ACTV_NUM', 'ACTIVE_WITH_BAL_NUM', 'FLAG_PURCH_NUM',
        'ACTV_FLAG_NUM', 'REVOLVER_NUM', 'CO_ALL_IND_NUM', 'CO_MNTH_IND_NUM'
    ]

    print("Processing SAS array flag to numeric conversion (do j = 1 to 8)...")
    for i in range(8): # Corresponds to "do j = 1 to 8"
        source_base_col_name = sas_flag_array_elements[i]
        target_col_name = sas_char_to_num_array_elements[i]

        actual_source_col_in_df = None
        for suffix in possible_suffixes:
            potential_name = f"{source_base_col_name}{suffix}"
            if potential_name in base_master_v2_df.columns:
                actual_source_col_in_df = potential_name
                break

        if actual_source_col_in_df:
            print(f"  Converting source column '{actual_source_col_in_df}' to target column '{target_col_name}'")
            base_master_v2_df[target_col_name] = np.where(
                base_master_v2_df[actual_source_col_in_df].astype(str).fillna('') == 'Y',
                1,
                0
            ).astype(int)
        else:
            print(f"  Warning: Source column for base name '{source_base_col_name}' not found. "
                  f"Defaulting target column '{target_col_name}' to 0.")
            base_master_v2_df[target_col_name] = 0

    # 2. BRONCO_FLAG logic
    print("Processing BRONCO_FLAG logic...")
    asset_class_cd_col_in_df = None
    for suffix in possible_suffixes:
        potential_name = f"ASSET_CLASS_CD{suffix}"
        if potential_name in base_master_v2_df.columns:
            asset_class_cd_col_in_df = potential_name
            break
            
    if asset_class_cd_col_in_df:
        print(f"  Using source column '{asset_class_cd_col_in_df}' for BRONCO_FLAG.")
        base_master_v2_df['BRONCO_FLAG'] = np.where(
            base_master_v2_df[asset_class_cd_col_in_df].astype(str).fillna('') != 'Sold',
            'NON-BRONCO',
            'BRONCO'
        )
    else:
        print(f"  Warning: Source column 'ASSET_CLASS_CD' not found. BRONCO_FLAG will be all None.")
        base_master_v2_df['BRONCO_FLAG'] = None

    # 3. cr_loss_net_amt logic
    print("Processing cr_loss_net_amt logic...")
    chrgbuff_amt_col_in_df = None
    recovry_actl_tot_amt_col_in_df = None

    for suffix in possible_suffixes:
        potential_chrg = f"CHRGBOFF_AMT{suffix}"
        if not chrgbuff_amt_col_in_df and potential_chrg in base_master_v2_df.columns:
            chrgbuff_amt_col_in_df = potential_chrg
        
        potential_recv = f"RECOVRY_ACTL_TOT_AMT{suffix}"
        if not recovry_actl_tot_amt_col_in_df and potential_recv in base_master_v2_df.columns:
            recovry_actl_tot_amt_col_in_df = potential_recv
        
        if chrgbuff_amt_col_in_df and recovry_actl_tot_amt_col_in_df: # Optimization
            break

    if chrgbuff_amt_col_in_df and recovry_actl_tot_amt_col_in_df:
        print(f"  Using '{chrgbuff_amt_col_in_df}' and '{recovry_actl_tot_amt_col_in_df}' for cr_loss_net_amt.")
        chrgbuff_amt_numeric = pd.to_numeric(base_master_v2_df[chrgbuff_amt_col_in_df], errors='coerce').fillna(0)
        recovry_actl_tot_amt_numeric = pd.to_numeric(base_master_v2_df[recovry_actl_tot_amt_col_in_df], errors='coerce').fillna(0)
        
        base_master_v2_df['cr_loss_net_amt'] = chrgbuff_amt_numeric + (recovry_actl_tot_amt_numeric * -1)
    else:
        print(f"  Warning: One or both source columns for 'cr_loss_net_amt' not found. It will be NaN/error prone.")
        base_master_v2_df['cr_loss_net_amt'] = np.nan

    # 4. NCL logic
    print("Processing NCL logic...")
    co_all_ind_num_col = 'CO_ALL_IND_NUM' # This was created by the array processing above
    
    chrg_status_rsn_cd_col_in_df = None
    for suffix in possible_suffixes:
        potential_name = f"CHRGOFF_STATUS_RSN_CD{suffix}"
        if potential_name in base_master_v2_df.columns:
            chrg_status_rsn_cd_col_in_df = potential_name
            break

    if co_all_ind_num_col in base_master_v2_df.columns and \
       chrg_status_rsn_cd_col_in_df and \
       'cr_loss_net_amt' in base_master_v2_df.columns: # Ensure cr_loss_net_amt was successfully created
        
        print(f"  Using '{co_all_ind_num_col}', '{chrg_status_rsn_cd_col_in_df}', and 'cr_loss_net_amt' for NCL.")
        
        condition_for_ncl = (
            (base_master_v2_df[co_all_ind_num_col] == 1) &
            (~base_master_v2_df[chrg_status_rsn_cd_col_in_df].astype(str).fillna('').isin(['FRD']))
        )
        
        # Ensure cr_loss_net_amt is numeric and NaNs are handled if the condition is met
        # If cr_loss_net_amt could be NaN from previous step, NCL would also be NaN
        # SAS's `NCL=CR_LOSS_NET_AMT` would carry over a missing if CR_LOSS_NET_AMT was missing
        base_master_v2_df['NCL'] = np.where(
            condition_for_ncl,
            base_master_v2_df['cr_loss_net_amt'], 
            0
        )
    else:
        missing_cols_for_ncl = []
        if co_all_ind_num_col not in base_master_v2_df.columns: missing_cols_for_ncl.append(co_all_ind_num_col)
        if not chrg_status_rsn_cd_col_in_df: missing_cols_for_ncl.append("CHRGOFF_STATUS_RSN_CD (base)")
        if 'cr_loss_net_amt' not in base_master_v2_df.columns: missing_cols_for_ncl.append('cr_loss_net_amt')
        print(f"  Warning: One or more source columns for 'NCL' ({', '.join(missing_cols_for_ncl)}) not found or cr_loss_net_amt not created. NCL will be 0 or NaN.")
        base_master_v2_df['NCL'] = 0 # Default to 0 as per SAS else NCL=0

    # --- End: Detailed logic from SAS image ---

    # --- Continue with other financial calculations from SAS code ---
    print("Processing remaining financial calculations...")
    # Example for NET_INTERCHANGE (ensure FIN_CHRG_GROSS_AMT etc. are handled for suffixes and numeric conversion)
    # fin_chrg_gross_amt_col = find_actual_column_name(base_master_v2_df, 'FIN_CHRG_GROSS_AMT', possible_suffixes)
    # fin_chrg_chgoff_rvsl_col = find_actual_column_name(base_master_v2_df, 'FIN_CHRG_CHGOFF_RVRSL_TOT_AMT', possible_suffixes)
    # if fin_chrg_gross_amt_col and fin_chrg_chgoff_rvsl_col:
    #     val1 = pd.to_numeric(base_master_v2_df[fin_chrg_gross_amt_col], errors='coerce').fillna(0)
    #     val2 = pd.to_numeric(base_master_v2_df[fin_chrg_chgoff_rvsl_col], errors='coerce').fillna(0)
    #     base_master_v2_df['NET_INTERCHANGE'] = val1 + val2
    # else:
    #     base_master_v2_df['NET_INTERCHANGE'] = np.nan
    #
    # ... Implement ALL other financial calculations (Gross_Yield, Rev_Bwd_MOD_new, Late_Fees, etc.)
    # ... following the pattern:
    # ... 1. Find actual column names (handling suffixes).
    # ... 2. Convert to numeric, filling NaNs with 0 (as SAS SUM does).
    # ... 3. Perform the calculation.
    # ... Handle cases where source columns are not found.

    # --- Delinquency Flags (DQ_p_unit, etc.) ---
    print("Processing delinquency flags...")
    # Example: (Ensure co_all_ind_num_col exists, delq_ind has suffixes handled, current_balance too)
    # delq_ind_col = find_actual_column_name(base_master_v2_df, 'delq_ind', possible_suffixes)
    # current_balance_col = find_actual_column_name(base_master_v2_df, 'current_balance', possible_suffixes)
    # if co_all_ind_num_col in base_master_v2_df.columns and delq_ind_col and current_balance_col:
    #    delq_ind_num = pd.to_numeric(base_master_v2_df[delq_ind_col], errors='coerce') # No fillna(0) yet
    #    current_balance_num = pd.to_numeric(base_master_v2_df[current_balance_col], errors='coerce').fillna(0)
    #
    #    cond_dq_p = (base_master_v2_df[co_all_ind_num_col] == 0) & \
    #                (delq_ind_num >= 2) & (delq_ind_num <= 7)
    #
    #    base_master_v2_df['DQ_p_unit'] = np.where(cond_dq_p, 1, 0)
    #    base_master_v2_df['DQ_p_dollar'] = np.where(cond_dq_p, current_balance_num, 0)
    # else:
    #    base_master_v2_df['DQ_p_unit'] = 0
    #    base_master_v2_df['DQ_p_dollar'] = 0
    # ... ALL other delinquency flag calculations ...


    # --- Prepare MCC Data (mcc_master_df from aspmstr.mcc_master) ---
    print("Preparing MCC data...")
    mcc_cols = ['acct_id', 'Period', 'amt_GAFINC', 'cnt_GAFINC', 'Points_GAFINC', 
                'net_trans_amt', 'net_trans_cnt', 'Points'] # Plus spend category columns
    # Add all category-specific columns from your mcc_master (e.g., amt_AIRLINES, cnt_AIRLINES etc.)
    # For example: mcc_cols.extend(['amt_AIRLINES', 'cnt_AIRLINES', 'Points_AIRLINES', ...])
    mcc_master_df = read_table_from_athena('aspmstr_mcc_master', columns=mcc_cols)
    
    if not mcc_master_df.empty:
        mcc_master_df['Period1_mcc'] = pd.to_datetime(mcc_master_df['Period'].astype(str) + '01', format='%Y%m%d', errors='coerce').dt.strftime('%Y%m')
        mcc_numeric_cols_to_clean = ['amt_GAFINC', 'cnt_GAFINC', 'Points_GAFINC', 
                                     'net_trans_amt', 'net_trans_cnt', 'Points']
        # Add all category-specific numeric columns (amt_AIRLINES, etc.) to mcc_numeric_cols_to_clean
        for col in mcc_numeric_cols_to_clean:
            if col in mcc_master_df.columns:
                 mcc_master_df[col] = pd.to_numeric(mcc_master_df[col], errors='coerce').fillna(0)
            else: # Handle if a column is expected but not in the table
                 mcc_master_df[col] = 0


        mcc_master_df['amt_NON_GAFINC'] = mcc_master_df['net_trans_amt'] - mcc_master_df['amt_GAFINC']
        mcc_master_df['cnt_NON_GAFINC'] = mcc_master_df['net_trans_cnt'] - mcc_master_df['cnt_GAFINC']
        mcc_master_df['Points_NON_GAFINC'] = mcc_master_df['Points'] - mcc_master_df['Points_GAFINC']
    else:
        print("Warning: mcc_master_df is empty after reading from Athena.")
        # Create an empty DataFrame with expected columns if joins will happen, to prevent errors
        # For now, subsequent merges will handle empty df on one side.

    # --- Final Major Joins -> main1_base_master_v3_df ---
    print("Performing final major joins...")
    fulfillment_cols = ['Campaign_id', 'acct_id', 'Period1'] # Add other necessary columns
    fulfillment_df = read_table_from_athena('fl_fulfillment', columns=fulfillment_cols)
    if not fulfillment_df.empty:
        fulfillment_df['Period1'] = fulfillment_df['Period1'].astype(str) # Ensure join key type match

    rwa_cols = ['acct_id', 'Period1'] # Add other necessary columns, e.g., 'RWA' value itself
    rwa_df = read_table_from_athena('rds_RWA', columns=rwa_cols)
    if not rwa_df.empty:
        rwa_df['Period1'] = rwa_df['Period1'].astype(str) # Ensure join key type match

    # Ensure base_master_v2_df.Period1 is also string for joins
    base_master_v2_df['Period1'] = base_master_v2_df['Period1'].astype(str)

    temp_df = base_master_v2_df # Start with the main df
    if not fulfillment_df.empty:
        temp_df = pd.merge(temp_df, fulfillment_df, on=['Campaign_id', 'acct_id', 'Period1'], how='left', suffixes=('', '_fulfill'))
    else:
        print("Skipping fulfillment merge as fulfillment_df is empty.")
        # Add placeholder columns if downstream code expects them from fulfillment_df

    if not mcc_master_df.empty:
        temp_df = pd.merge(temp_df, mcc_master_df, left_on=['acct_id', 'Period1'], right_on=['acct_id', 'Period1_mcc'], how='left', suffixes=('', '_mcc'))
    else:
        print("Skipping MCC merge as mcc_master_df is empty.")
        # Add placeholder columns if downstream code expects them from mcc_master_df

    if not rwa_df.empty:
        main1_base_master_v3_df = pd.merge(temp_df, rwa_df, on=['acct_id', 'Period1'], how='left', suffixes=('', '_rwa'))
    else:
        print("Skipping RWA merge as rwa_df is empty.")
        main1_base_master_v3_df = temp_df # If RWA is last, assign temp_df
        # Add placeholder columns if downstream code expects them from rwa_df
    
    save_df_to_s3(main1_base_master_v3_df, "main1_base_master_v3_df", "Fully Enriched Base Data")
    print("--- Part 2: Financial Enrichment and Further Joins Completed ---")
    return "main1_base_master_v3_df"

# Helper function to find column name (example, you might want to make it more robust)
def find_actual_column_name(df, base_name, suffixes):
    for suffix in suffixes:
        potential_name = f"{base_name}{suffix}"
        if potential_name in df.columns:
            return potential_name
    print(f"Warning: Base column '{base_name}' not found with any known suffix.")
    return None
import os
import numpy as np
import pandas as pd

excel_path = os.path.join(docs_location, scen_file)

for iter_fx_pairs in range(num_crncy_pairs):
    # -------- dates for this pair (begin/end from your control table) --------
    str_data_bgn = run_data_summary.iloc[iter_fx_pairs, 1]
    str_data_end = run_data_summary.iloc[iter_fx_pairs, 2]

    # -------- resolve source sheet and read daily data --------
    src_sheet_try = arr_lbty_asst_crncy_pairs[iter_fx_pairs]  # e.g. 'AsstLiab_USDJPY'
    try:
        arr_data = pd.read_excel(os.path.join(data_location, data_file),
                                 sheet_name=src_sheet_try)
        print(f"Sheet '{src_sheet_try}' found.")
    except ValueError:
        print(f"Sheet '{src_sheet_try}' not found. Skipping.")
        continue

    # first col as datetime, rest keep as is
    arr_data.iloc[:, 0] = pd.to_datetime(arr_data.iloc[:, 0])

    # -------- filter to in-sample window --------
    date_mask = (arr_data.iloc[:, 0] >= pd.to_datetime(str_data_bgn)) & \
                (arr_data.iloc[:, 0] <= pd.to_datetime(str_data_end))
    # daily dates & levels (in-sample)
    cell_array_calendar_dates = arr_data.loc[date_mask, arr_data.columns[0]].to_numpy()
    data = arr_data.loc[date_mask, arr_data.columns[1]].to_numpy()

    # -------- convert daily -> monthly levels (use your convention) --------
    # this monthly series is the base for BOTH scenarios and HistoricalDep
    col = convert_daily_to_monthly_time_series(
        cell_array_calendar_dates, data, 'avg'  # or 'eom'
    )

    # -------- build scenario rows (AAA..B) at the requested horizons --------
    # results shape expected: (num_crncy_pairs, num_monthly_prds, len(arr_rtg_cat_list))
    for exp_win_len in range(scen_freq_months, num_monthly_prds + 1, scen_freq_months):
        lag = exp_win_len
        if len(col) >= lag:
            g = get_returns(col, exp_win_len, 1)           # 1 = simple return
            BBB = np.percentile(g, 100 * tail_probs_BBB)   # your tail prob for BBB
            B   = max(BBB, 0)
            BB  = max(0, 0.5 * BBB)
            A   = max(stressor * BBB, max(B))              # follow your stress rule
            AAA = min(A, 1)                                 # cap at 100% if that's your rule
            AA  = (2/3) * AAA + (1/3) * BBB
            A_  = (1/3) * AAA + (2/3) * BBB
            # order to match arr_rtg_cat_list = ['AAA','AA','A','BBB','BB','B']
            results[iter_fx_pairs, exp_win_len - 1, :] = [AAA, AA, A_, BBB, BB, B]
        else:
            print(f"Warning: Not enough data for lag={exp_win_len}. len(col)={len(col)}")

    # -------- assemble output DataFrame (percent, rounded) --------
    ccy1 = arr_lbty_asst_crncy_pairs[iter_fx_pairs][-6:-3]
    ccy2 = arr_lbty_asst_crncy_pairs[iter_fx_pairs][-3:]
    sheet_name = f"{ccy1}Asts_DevalueWrt_{ccy2}Lbts"

    mat_crncy_pair_scenarios = results[iter_fx_pairs, :num_monthly_prds, :] * 100.0
    mat_crncy_pair_scenarios_appx = np.rint(mat_crncy_pair_scenarios)
    df_out = pd.DataFrame(mat_crncy_pair_scenarios_appx, columns=arr_rtg_cat_list)

    # -------- HistoricalDep (in-sample) from the SAME monthly series --------
    # if get_returns already returns percent, remove the *100.0
    hist_rets = get_returns(col, lag=1, return_type=1) * 100.0
    historical_dep = pd.Series(hist_rets, index=pd.to_datetime(col.index[1:])).dropna()
    historical_dep.name = "HistoricalDep"

    # align to model rows
    n = min(len(df_out), len(historical_dep))
    df_out = df_out.iloc[:n].reset_index(drop=True)
    hist_col = historical_dep.iloc[:n].reset_index(drop=True)
    # put HistoricalDep as the first column (move to end if you prefer)
    df_out.insert(0, "HistoricalDep", hist_col)

    # -------- write to Excel --------
    mode = 'a' if os.path.exists(excel_path) else 'w'
    with pd.ExcelWriter(excel_path, engine='openpyxl', mode=mode, if_sheet_exists='replace') as writer:
        df_out.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Wrote {sheet_name} with HistoricalDep ({n} rows) -> {excel_path}")

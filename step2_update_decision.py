import os
import pandas as pd
from pathlib import Path

def get_sign(x):
    if x > 0: return 1
    elif x < 0: return -1
    else: return 0

""" delta thresholds """
def get_delta_thresholds(basin_level, folder, area, alpha):
    df_thd = pd.read_csv(f"outputs_utest_V1/{folder}/{area}/basin_level_mean_std.csv").set_index('basin_level')
    mean = df_thd[f'mean'].loc[basin_level]
    std = df_thd[f'std'].loc[basin_level]
    print(f"mean: {mean}, std: {std}")

    thd_low = mean - alpha * std 
    thd_high = mean + alpha * std 
    print(f"thd_low: {thd_low}, thd_high: {thd_high}")
    return mean, std, thd_low, thd_high

""" make decision based on delta and utest, add a column named decision """
def add_decision_V0(df, thd_low, thd_high):
    df['decision'] = None

    # negatively changed basins agreed by both delta and u-test
    df.loc[(df.delta < thd_low) & (df.p_u < p_thd), 'decision'] = -1 

    # positively changed basins agreed by both delta and u-test
    df.loc[(df.delta > thd_high) & (df.p_u < p_thd), 'decision'] = 1 

    # unchanged basins suggested by one of delta and u-test or both of them
    df.loc[((df.delta >= thd_low) & (df.delta <= thd_high)) | (df.p_u >= p_thd), 'decision'] = 0 
    
    # dry basins
    df.loc[df.baseline_median < 0.0225, 'decision'] = -99 

    # old version
    if False:
        df.loc[(df.p_u < p_thd) & (df.delta < 0), 'decision'] = -1 # decreased basins determined by utest
        df.loc[(df.p_u < p_thd) & (df.delta > 0), 'decision'] = 1 # increased basins  determined by utest
        df.loc[df.p_u >= p_thd, 'decision'] = 0 # non-change determined by u-test
        df.loc[(thd_low <= df.delta) & (df.delta <= thd_high), 'decision'] = 0 # non-change determined by delta mean and std
        df.loc[df.baseline_median < 0.0225, 'decision'] = -99 # dry basins

    return df


def add_decision(df, thd_low, thd_high):
    df['decision'] = None

    # negatively changed basins agreed by both delta and u-test
    df.loc[(df.delta < thd_low), 'decision'] = -1 

    # positively changed basins agreed by both delta and u-test
    df.loc[(df.delta > thd_high), 'decision'] = 1 

    # unchanged basins suggested by one of delta and u-test or both of them
    df.loc[((df.delta >= thd_low) & (df.delta <= thd_high)), 'decision'] = 0 

    # dry basins
    df.loc[df.baseline_median < 0.0225, 'decision'] = -99 
    

    return df




p_thd = 0.1 # p_thd = 0.025
alpha = 1

input_dir = Path("outputs_decision")
for folder in ['Permanent_water', 'Reservoirs']:
    
    if 'Permanent_water' == folder: alpha = 1.5
    if 'Reservoirs' == folder: alpha = 1.0

    for area in ['permanent_area', 'seasonal_area']:
        # save_dir = input_dir / 'updated_decision' / folder / area
        save_dir = input_dir / 'delta_decision' / folder / area
        save_dir.mkdir(exist_ok=True, parents=True)

        if 'seasonal_area' == area: alpha = 2.5

        # for basin_level in [0, 3, 4, 5, 6, 7, 8]:
        for basin_level in [6]:
            print()
            print(f"{folder}/{area}/basin_level: {basin_level}/alpha: {alpha}/p_thd: {p_thd}")

            df = pd.read_csv( input_dir / folder / area / f"basins_level_{basin_level}_utest.csv")
            # mean, std, thd_low, thd_high = get_delta_thresholds(basin_level, folder, area, alpha)

            # get_delta_thresholds
            mean = df[f'mean'].iloc[0]
            std = df[f'std'].iloc[0]
            print(f"mean: {mean}, std: {std}")

            thd_low = mean - alpha * std 
            thd_high = mean + alpha * std 
            print(f"thd_low: {thd_low}, thd_high: {thd_high}")

            # make decision and add it as a new column
            df = add_decision(df, thd_low, thd_high)

            print(df.decision.unique())
            dry = df[df.decision == -99].shape[0]
            neg = df[df.decision == -1].shape[0]
            stable = df[df.decision == 0].shape[0]
            pos = df[df.decision == 1].shape[0]
            print(f"total: {df.shape[0]}, neg: {neg}, stable: {stable}, pos: {pos}, dry: {dry}")

            df['mean'] = mean
            df['std'] = std
            df['thd_low'] = thd_low
            df['thd_high'] = thd_high
            df['alpha'] = alpha
            df['p_thd'] = p_thd

            df = df[['id_bgl', 'basin_level', 'start_year', 't_score', 'p_t', 'u_score', 'mean', 'std', 'alpha', 
                     'thd_low', 'thd_high', 'baseline_median', 'delta','p_u', 'p_thd', 'decision']]
            
            df.set_index('id_bgl').to_csv(save_dir / f"basins_level_{basin_level}_utest.csv")
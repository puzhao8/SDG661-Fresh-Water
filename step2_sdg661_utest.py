''' dask based '''
import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import dask
import dask.dataframe as dd
import scipy.stats as stats

import warnings
warnings.filterwarnings("ignore")

dtypes = {"aggregation_year": 'uint16', "permanent_area": 'float64',
          "seasonal_area": 'float64',
          "maybepermanent": 'float64',
          "maybeseasonal": 'float64'
         }

COL_NAME = "seasonal_area" # permanent_area
cols_required = [
    COL_NAME
    # 'permanent_area', 
    # 'seasonal_area'
    #  'maybepermanent', 'maybeseasonal'
                ]



meta = {'id_bgl': 'str', 
        'basin_level': int, 'start_year': int, 
        't_score': 'float', 'u_score': 'float', 'p_t': 'float', 'p_u': 'float', 
        'sign_diff': 'float', 'delta': 'float'
       }
# meta_adm0 = {'id': 'str', 'adm0_name': 'str', 't_score': 'float', 'u_score': 'float', 'p_t': 'float', 'p_u': 'float', 'delta': 'float'} #'id_bgl': 'object', 'start_year': int, 'basin_level': int
# meta.update({col: 'float16' for col in cols_required})

def t_test_and_u_test(group, basin_level):
    id = group.index[0] # basin_id
    
    patch = group[(group.index == id) & (group['aggregation_year']  >=2000)]
    baseline_period = list(patch[patch['aggregation_year'] < 2020][COL_NAME].values)
    report_period = list(patch[patch['aggregation_year'] >= 2017][COL_NAME].values)

    # T-test
    t_score, p_t = stats.ttest_ind(report_period, baseline_period,  equal_var=False)

    # U-Test
    u_score, p_u = stats.mannwhitneyu(report_period, baseline_period)
    median_report = np.median(report_period)
    median_baseline = np.median(baseline_period)
    median_diff = median_report - median_baseline
    sign_diff = median_diff / np.abs(median_diff)

    # delta
    delta = (median_report - median_baseline) / (median_baseline + 1e-15) * 100

    # p_u_thd = float(p_u < p_thd)
    df = pd.DataFrame([[id, basin_level, 2017, t_score, u_score, p_t, p_u, sign_diff, delta]], 
                    columns=['id_bgl', 'basin_level', 'start_year', 't_score', 'u_score', 'p_t', 'p_u', 'sign_diff', 'delta'])
    return df



if __name__ == '__main__':
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(dashboard_address=':38787')
    client = Client(cluster)#timeout

    data_dir =  Path('data')
    arr_mean_std = []
    
    for folder in ["Pemanent_water", "Reservoirs"]: # Reservoirs, Pemanent_water
    
        output_dir = Path("outputs_utest") / folder / COL_NAME 
        output_dir.mkdir(exist_ok=True, parents=True)
        print(output_dir)
    
        # basin-level analysis
        for basin_level in [0, 3, 4, 5, 6, 7, 8]:
        # for basin_level in [3]:
            print()
            print(f"basins_level: {basin_level}")
        
            url = data_dir / folder / f"basins_level_{basin_level}_ts.csv"
            basin = dd.read_csv(url, include_path_column=False, dtype=dtypes).repartition(npartitions=80).set_index(f'id_bgl_{basin_level}')
            
            ''' for debug '''
            # number = 31
            # df_delta = basin.groupby(f'id_bgl_{basin_level}', group_keys=False)
            # t_test_and_u_test(df_delta.get_group("112_262").compute(), basin_level)
        
            # df_delta = basin.groupby(f'id_bgl_{basin_level}', group_keys=False).apply(calculate_delta, basin_level, epision, meta=meta)
            df_utest = basin.groupby(f'id_bgl_{basin_level}', group_keys=False).apply(t_test_and_u_test, basin_level, meta=meta).set_index('id_bgl')
           
            df_utest = df_utest.compute()
            df_utest.to_csv(output_dir / f"basins_level_{basin_level}_utest.csv")

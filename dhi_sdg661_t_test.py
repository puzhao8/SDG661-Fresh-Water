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

data_dir = Path.home() / 'pz'
cols_required = ['permanent_area', 
                 # 'seasonal_area'
                #  'maybepermanent', 'maybeseasonal'
                ]
arr_mean_std = []

meta = {'id': 'str', 't_score': 'float', 'u_score': 'float', 'p_t': 'float', 'p_u': 'float', 'reject_null': 'bool'} #'id_bgl': 'object', 'start_year': int, 'basin_level': int
meta_adm0 = {'id': 'str', 'adm0_name': 'str', 't_score': 'float', 'u_score': 'float', 'p_t': 'float', 'p_u': 'float', 'reject_null': 'bool'} #'id_bgl': 'object', 'start_year': int, 'basin_level': int
# meta.update({col: 'float16' for col in cols_required})

def t_test_and_u_test(group, p_thd = 0.001):
    id = group.index[0] # basin_id
    
    patch = group[(group.index == id) & (group['aggregation_year']  >=2000)]
    data1 = list(patch[patch['aggregation_year'] < 2020]['permanent_area'].values)
    data2 = list(patch[patch['aggregation_year'] >= 2017]['permanent_area'].values)

    # T-test
    t_score, p_t = stats.ttest_ind(data1, data2)

    # U-Test
    u_score, p_u = stats.mannwhitneyu(data1, data2)

    # P-value is defined as the probability under the assumption of no effect or no difference (null hypothesis), 
    # of obtaining a result equal to or more extreme than what was actually observed
    # P-values close to 0 indicate that the observed difference is unlikely to be due to chance, (true difference?)
    # whereas a P value close to 1 suggests no difference between the groups other than due to chance (no difference?)
    reject_null = (p_t < p_thd) or (p_u < p_thd) # True: different, False: no difference

    df = pd.DataFrame([[id, t_score, u_score, p_t, p_u, float(reject_null)]], columns=['id', 't_score', 'u_score', 'p_t', 'p_u', 'reject_null'])
    return df




if __name__ == '__main__':
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(dashboard_address=':38787')
    client = Client(cluster)#timeout

    for folder in ["Pemanent_water"]: # Reservoirs, Pemanent_water

        output_dir = data_dir / f"outputs_ttest" / folder
        output_dir.mkdir(exist_ok=True, parents=True)
        print(output_dir)

        # # country-level analysis
        # basin = dd.read_csv(data_dir / folder / 'gaul_0_ts.csv', include_path_column=False, dtype=dtypes).repartition(npartitions=80).set_index('adm0_code')
        # df_delta = basin.groupby('adm0_code', group_keys=False).apply(t_test_and_u_test, meta=meta_adm0)
        # df_delta.compute().to_csv(output_dir / f"gaul_0_t_test.csv")

        # basin-level analysis
        for basin_level in [0, 3, 4, 5, 6, 7, 8]:
            print()
            print(f"basins_level: {basin_level}")
        
            url = data_dir / folder / f"basins_level_{basin_level}_ts.csv"
            basin = dd.read_csv(url, include_path_column=False, dtype=dtypes).repartition(npartitions=80).set_index(f'id_bgl_{basin_level}')
            
            ''' for debug '''
            # number = 31
            # df_delta = basin.groupby(f'id_bgl_{basin_level}', group_keys=False)
            # t_test_and_u_test(df_delta.get_group("112_262").compute(), basin_level)
        
            # df_delta = basin.groupby(f'id_bgl_{basin_level}', group_keys=False).apply(calculate_delta, basin_level, epision, meta=meta)
            df_delta = basin.groupby(f'id_bgl_{basin_level}', group_keys=False).apply(t_test_and_u_test, meta=meta).set_index('id')
        
            df_delta = df_delta.compute()
            df_delta.to_csv(output_dir / f"basins_level_{basin_level}_t_test.csv")

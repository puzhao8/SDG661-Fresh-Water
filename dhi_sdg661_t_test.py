import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import dask
import dask.dataframe as dd

dtypes = {"aggregation_year": 'uint16',"permanent_area": 'float64',"seasonal_area": 'float64',
          "maybepermanent": 'float64',"maybeseasonal": 'float64'
         }

data_dir = Path.home() / 'pz'
cols_required = ['permanent_area', 
                 # 'seasonal_area'
                #  'maybepermanent', 'maybeseasonal'
                ]
arr_mean_std = []

meta = {'id_bgl': 'object', 'start_year': int, 'basin_level': int} #'id_bgl': 'object', 'start_year': int, 'basin_level': int
meta.update({col: 'float16' for col in cols_required})

def t_test_and_u_test(group, basin_level):
    id = group.index[0] # basin_id
    patch = basin[(basin[id_col_name] == id) & (basin['aggregation_year']  >=2000)]
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
    df = pd.DataFrame([id, t_score, u_score, p_t, p_u, reject_null], columns=['id', 't_score', 'u_score', 'p_t', 'p_u', 'reject_null'])
    return df


def calculate_delta(group, basin_level, epision=1e-15):
    # group = group.reset_index() # needed for drop
    id = group.index[0] # basin_id
    
    df_2000_2019 = group[ (2000 <= group['aggregation_year']) & (group['aggregation_year']<= 2019)]
    beta_baseline_median = df_2000_2019[cols_required].median()

    arr_delta = []
    for start_year in [2000, 2005, 2010, 2015, 2017]:
    # for start_year in [2017]:
        df_5y = group[ (start_year <= group['aggregation_year']) & (group['aggregation_year'] < start_year + 5)]
        gamma_5y_median = df_5y[cols_required].median()
        
        delta = (gamma_5y_median - beta_baseline_median) / (beta_baseline_median + epision) * 100

        delta_cols = list([delta[col].values for col in [cols_required]][0])
        arr_delta.append([id, start_year, basin_level] + delta_cols)

    df_delta = pd.DataFrame(data=arr_delta, columns=['id_bgl', 'start_year', 'basin_level'] + cols_required)
    return df_delta

def remove_outliers(df, p_low, p_high):
    for col in ['permanent_area']:
        select = (df[col] > p_low[col]) & (df[col] < p_high[col])
        # select = (df[col] < p_high[col])
        df = df[select]
        # df = df.drop(index=df[df[col] <= p1[col]].index)
        # df = df.drop(index=df[df[col] >= p99[col]].index)
    return df

if __name__ == '__main__':
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(dashboard_address=':38787')
    client = Client(cluster)#timeout

    folder = "Reservoirs" # Reservoirs, Pemanent_water
    epision = 1e-15
    
    if "Pemanent_water" == folder:
        p_low = 2 # remove lowest 1%
        p_high = 98 # remove highest 4%

    if "Reservoirs" == folder:
        p_low = 2 # remove lowest 1%
        p_high = 98 # remove highest 4%
    
    
    output_dir = data_dir / f"outputs_V2_p{p_low}_p{p_high}_e15" / folder
    output_dir.mkdir(exist_ok=True, parents=True)
    print(output_dir)

    for basin_level in range(3, 9):
        print()
        print(f"basins_level: {basin_level}")
    
        url = data_dir / folder / f"basins_level_{basin_level}_ts.csv"
        basin = dd.read_csv(url, include_path_column=False, dtype=dtypes).repartition(npartitions=80).set_index(f'id_bgl_{basin_level}')
        

        # df_delta = basin.groupby(f'id_bgl_{basin_level}', group_keys=False).apply(calculate_delta, basin_level, epision, meta=meta)
        df_delta = basin.groupby(f'id_bgl_{basin_level}', group_keys=False).apply(t_test_and_u_test, basin_level, meta=meta)
        df_delta = df_delta.compute()

        p_low_value = df_delta[cols_required].quantile(p_low * 0.01)#.compute()
        p_high_value = df_delta[cols_required].quantile(p_high * 0.01)#.compute()

        print(f"--------------------------------- basin_level: {basin_level}-----------------------------------")
        print('p_low')
        print(p_low_value)
        print('p_high')
        print(p_high_value)

        df_delta.to_csv(output_dir / f"basins_level_{basin_level}_ts_delta.csv")
        
        df = remove_outliers(df_delta, p_low_value, p_high_value)
        print(df.describe())
        
        # df_delta.visualize('test.png')
        df.to_csv(output_dir / f"basins_level_{basin_level}_ts_delta_outliers_removed.csv")

        # save hist
        import matplotlib.pyplot as plt
        for col in cols_required:
            plt.figure()
            df[col].plot(kind='hist', logy=True, bins=200)
            plt.title(f'basin level: {basin_level}, {col}')
            plt.savefig(output_dir / f"basins_level_{basin_level}_hist_{col}.png")
            plt.close()

        
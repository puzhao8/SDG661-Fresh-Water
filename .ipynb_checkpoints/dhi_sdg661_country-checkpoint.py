import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import dask
import dask.dataframe as dd

dtypes = {"aggregation_year": 'uint16',
          "permanent_area": 'float64', 
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

meta = {'id_bgl': 'object', 'adm0_name': 'object', 'start_year': int, 'basin_level': int} #'id_bgl': 'object', 'start_year': int, 'basin_level': int
meta.update({col: 'float16' for col in cols_required})


def calculate_delta(group, basin_level, epision=1e-15):
    # group = group.reset_index() # needed for drop
    id = group.index[0]
    adm0_name = group['adm0_name'].iloc[0]
    
    df_2000_2019 = group[ (2000 <= group['aggregation_year']) & (group['aggregation_year']<= 2019)]
    beta_baseline_median = df_2000_2019[cols_required].median()

    arr_delta = []
    # for start_year in [2000, 2005, 2010, 2015, 2017]:
    for start_year in [2017]:
        df_5y = group[ (start_year <= group['aggregation_year']) & (group['aggregation_year'] < start_year + 5)]
        gamma_5y_median = df_5y[cols_required].median()

        # solution 1: 
        delta = (gamma_5y_median - beta_baseline_median) / (beta_baseline_median + epision) * 100

        delta_cols = list([delta[col].values for col in [cols_required]][0])
        arr_delta.append([id, adm0_name, start_year, basin_level] + delta_cols)
    
    df_delta = pd.DataFrame(data=arr_delta, columns=['id_bgl', 'adm0_name', 'start_year', 'basin_level'] + cols_required)
    return df_delta

def remove_outliers(df, p_low, p_high):
    for col in ['permanent_area']:
        select = (df[col] > p_low[col]) & (df[col] < p_high[col])
        df = df[select]
    return df


if __name__ == '__main__':
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(dashboard_address=':38787')
    client = Client(cluster)#timeout

    p_low = 2 # remove lowest 1%
    p_high = 98 # remove highest 4%
    epision = 1e-15
    
    for folder in  ["Reservoirs", "Pemanent_water"]: # Reservoirs, Pemanent_water
        output_dir = data_dir / f"outputs_V2_p{p_low}_p{p_high}_e15_2017" / folder
        output_dir.mkdir(exist_ok=True, parents=True)
        print(output_dir)
    
        # for basin_level in range(3,9):
    
        basin_level = 'gaul_0'
        url = data_dir / folder / f"gaul_0_ts.csv"
        basin = dd.read_csv(url, include_path_column=False, dtype=dtypes).repartition(npartitions=80).set_index('adm0_code')
        
        df_delta = basin.groupby('adm0_code', group_keys=False).apply(calculate_delta, basin_level, epision, meta=meta)
        df_delta = df_delta.compute()
    
        # remove the rows where delta >= 100, since we assume that this may be caused by the case when  
        df_delta_ = df_delta[cols_required][df_delta['permanent_area'] <= 100]
        p_low_value = df_delta_[cols_required].quantile(p_low * 0.01)#.compute()
        p_high_value = df_delta_[cols_required].quantile(p_high * 0.01)#.compute()
    
        print('p_low')
        print(p_low_value)
        print('p_high')
        print(p_high_value)
    
        df_delta.to_csv(output_dir / f"{basin_level}_ts_delta.csv")
        df = remove_outliers(df_delta_, p_low_value, p_high_value)
        
        print()
        print(df.describe())
        df.to_csv(output_dir / f"{basin_level}_ts_delta_outliers_removed.csv")
    
        # save hist
        import matplotlib.pyplot as plt
        for col in cols_required:
            plt.figure()
            df[col].plot(kind='hist', logy=True, bins=200)
            plt.title(f'basin level: {basin_level}, {col}')
            plt.savefig(output_dir / f"{basin_level}_hist_{col}.png")
            plt.close()
    
        # calculate mean and std
        row = [basin_level] + list(df[cols_required].mean().values) + list(df[cols_required].std().values)
        arr_mean_std.append(row)
    
        col_names = ['basin_level'] + [f'mean_{col}' for col in cols_required] + [f'std_{col}' for col in cols_required]
        print(arr_mean_std)
        
        df_mean_std = pd.DataFrame(data=arr_mean_std, columns=col_names)
        # df_mean_std.to_csv(output_dir / f"{basin_level}_mean_std.csv")
        df_mean_std.to_csv(output_dir / f"gaul_0_mean_std.csv")
        print("finish")
        
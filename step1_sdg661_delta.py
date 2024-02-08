import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import dask
import dask.dataframe as dd

dtypes = {"aggregation_year": 'uint16',"permanent_area": 'float64',"seasonal_area": 'float64',
          "maybepermanent": 'float64',"maybeseasonal": 'float64'
         }

data_dir = Path('data')
COL_NAME = 'seasonal_area'
cols_required = [
    COL_NAME
    # 'permanent_area', 
    # 'seasonal_area'
    #  'maybepermanent', 'maybeseasonal'
    ]
arr_mean_std = []

meta = {'id_bgl': 'object', 'start_year': int, 'basin_level': int} #'id_bgl': 'object', 'start_year': int, 'basin_level': int
meta.update({col: 'float16' for col in cols_required})


def calculate_delta(group, basin_level, epision=1e-15):
    # group = group.reset_index() # needed for drop
    id = group.index[0]
    
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
    for col in [COL_NAME]:
        select = (df[col] > p_low[col]) & (df[col] < p_high[col])
        df = df[select]
    return df



if __name__ == '__main__':
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(dashboard_address=':38787')
    client = Client(cluster)#timeout
    
    folder = "Reservoirs" # Reservoirs, Pemanent_water
    epision = 1e-15
    
    p_low = 2 # remove lowest 1%
    p_high = 98 # remove highest 4%
    
    output_dir = Path("outputs_delta") / folder / COL_NAME 
    output_dir.mkdir(exist_ok=True, parents=True)
    print(output_dir)
    
    for basin_level in [0, 3, 4, 5, 6, 7, 8]:
        print()
        print(f"basins_level: {basin_level}")
    
        url = data_dir / folder / f"basins_level_{basin_level}_ts.csv"
        basin = dd.read_csv(url, include_path_column=False, dtype=dtypes).repartition(npartitions=80).set_index(f'id_bgl_{basin_level}')
        
        df_delta = basin.groupby(f'id_bgl_{basin_level}', group_keys=False).apply(calculate_delta, basin_level, epision, meta=meta).set_index('id_bgl')
        df_delta = df_delta.compute()
        df_delta.to_csv(output_dir / f"basins_level_{basin_level}_ts_delta.csv")
    
        # remove the rows where delta > 100, since we assume that this may be caused by the case when  
        df_delta = df_delta[df_delta[COL_NAME] <= 100]        
        p_low_value = df_delta[cols_required].quantile(p_low * 0.01)#.compute()
        p_high_value = df_delta[cols_required].quantile(p_high * 0.01)#.compute()
    
        print(f"--------------------------------- basin_level: {basin_level}-----------------------------------")
        print(f"p_low_value: {p_low_value[COL_NAME]}")
        print(f"p_high_value: {p_high_value[COL_NAME]}")
        
        df = remove_outliers(df_delta, p_low_value, p_high_value)
        print()
        print(df.describe())
        
        # df_delta.visualize('test.png')
        # df.to_csv(output_dir / f"basins_level_{basin_level}_ts_delta_outliers_removed.csv")
    
        # save hist
        import matplotlib.pyplot as plt
        for col in cols_required:
            plt.figure()
            df[col].plot(kind='hist', logy=True, bins=200)
            plt.title(f'basin level: {basin_level}, {col}')
            plt.savefig(output_dir / f"basins_level_{basin_level}_hist_{col}.png")
            plt.close()
    
        # calculate mean and std
        row = [basin_level] + list(df[cols_required].mean().values) + list(df[cols_required].std().values)
        arr_mean_std.append(row)
    
        col_names = ['basin_level'] + [f'mean_{col}' for col in cols_required] + [f'std_{col}' for col in cols_required]
        df_mean_std = pd.DataFrame(data=arr_mean_std, columns=col_names).set_index('basin_level')
        # df_mean_std.to_csv(output_dir / f"basin_level_{basin_level}_mean_std.csv")
        df_mean_std.to_csv(output_dir / f"basin_level_mean_std.csv")
        
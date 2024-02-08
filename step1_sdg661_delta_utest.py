import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import dask
import dask.dataframe as dd
import scipy.stats as stats

dtypes = {
            "aggregation_year": 'uint16', 
            "permanent_area": 'float64',
            "seasonal_area": 'float64',
            "maybepermanent": 'float64',
            "maybeseasonal": 'float64'
         }

# cols_required = [
#     area
#     # 'permanent_area', 
#     # 'seasonal_area'
#     #  'maybepermanent', 'maybeseasonal'
#     ]


meta = {
    'id_bgl': 'str', 
    'basin_level': int, 
    'start_year': int, 
    't_score': 'float', 
    'p_t': 'float', 
    'u_score': 'float', 
    'p_u': 'float', 
    'baseline_median': float,
    'delta': float,
    }
# meta.update({col: 'float16' for col in cols_required})


def delta_and_utest(group, basin_level, epision=1e-5):
    # group = group.reset_index() # needed for drop
    id = group.index[0]
    
    baseline_period = group[(2000 <= group['aggregation_year']) & (group['aggregation_year']<= 2019)][area]
    baseline_median = baseline_period.median()

    row_arr = []
    for start_year in [2000, 2005, 2010, 2015, 2017]:
    # for start_year in [2017]:
        report_period = group[ (start_year <= group['aggregation_year']) & (group['aggregation_year'] < start_year + 5)][area]
        report_median = report_period.median()

        t_score, p_t = stats.ttest_ind(report_period, baseline_period,  equal_var=False) # T-test
        u_score, p_u = stats.mannwhitneyu(report_period, baseline_period) # U-Test
        
        delta = (report_median - baseline_median) / (baseline_median + epision) * 100
        row_arr.append([id, basin_level, start_year, t_score, p_t, u_score, p_u, baseline_median, delta])

    df = pd.DataFrame(data=row_arr, 
        columns=['id_bgl', 'basin_level', 'start_year', 't_score', 'p_t', 'u_score', 'p_u', 'baseline_median', 'delta'])

    return df

def remove_outliers(df, low, high):
    return df[(low <= df['delta']) & (df['delta'] <= high)]


if __name__ == '__main__':
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(n_workers=8, threads_per_worker=4, dashboard_address=':38787')
    client = Client(cluster)#timeout
    
    data_dir = Path('data')
    folder = "Reservoirs" # Reservoirs, Permanent_water
    area = 'seasonal_area' # permanent_area, seasonal_area

    epision = 1e-5 # 0.0225
    
    p_low = 2 # remove lowest 1%
    p_high = 98 # remove highest 4%
    
    output_dir = Path("outputs_utest_V1") / folder / area 
    output_dir.mkdir(exist_ok=True, parents=True)
    print(output_dir)
    
    arr_mean_std = []
    for basin_level in [0, 3, 4, 5, 6, 7, 8]:
    # for basin_level in [0, 3]:
        print()
        print(f"basins_level: {basin_level}")
    
        url = data_dir / folder / f"basins_level_{basin_level}_ts.csv"
        basin = dd.read_csv(url, include_path_column=False, dtype=dtypes).repartition(npartitions=80).set_index(f'id_bgl_{basin_level}')
        
        df_delta = basin.groupby(f'id_bgl_{basin_level}', group_keys=False).apply(delta_and_utest, basin_level, epision, meta=meta).set_index('id_bgl')
        df_delta = df_delta.compute()
        df_delta.to_csv(output_dir / f"basins_level_{basin_level}_utest.csv")
    
        # remove the rows where delta > 100, since we assume that this may be caused by the case when  
        df_delta_cut = df_delta[(-100 <= df_delta['delta']) & (df_delta['delta'] <= 100)]        
        low = df_delta_cut['delta'].quantile(p_low * 0.01)#.compute()
        high = df_delta_cut['delta'].quantile(p_high * 0.01)#.compute()
    
        print(f"--------------------------------- basin_level: {basin_level}-----------------------------------")
        print(f"low: {low}")
        print(f"high: {high}")
        
        df = remove_outliers(df_delta, low, high)
        
        print()
        print("df describe after removing outliers")
        print(df.describe())
        
        # df_delta.visualize('test.png')
        # df.to_csv(output_dir / f"basins_level_{basin_level}_ts_delta_outliers_removed.csv")
    
        # save hist
        import matplotlib.pyplot as plt
        plt.figure()
        df['delta'].plot(kind='hist', logy=True, bins=200)
        plt.title(f'basin level: {basin_level}, {area}')
        plt.savefig(output_dir / f"basins_level_{basin_level}_hist_{area}.png")
        plt.close()
    
        # calculate mean and std
        row = [basin_level, df['delta'].mean(), df['delta'].std()]
        arr_mean_std.append(row)

        df_mean_std = pd.DataFrame(data=arr_mean_std, columns=['basin_level', 'mean', 'std']).set_index('basin_level')
        df_mean_std.to_csv(output_dir / f"basin_level_mean_std.csv")
        
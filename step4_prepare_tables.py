#%%
import os
import pandas as pd
from pathlib import Path

def get_delta_at_basin_level_0(input_dir, folder, area):
    delta_basin_0 = pd.read_csv(input_dir / folder / area / "basins_level_0_utest.csv")
    delta_0 = delta_basin_0[['id_bgl', 'start_year', 'delta']].rename(columns={'id_bgl': 'adm0_code'})

    def rearrange_by_period(group): 
        period_delta = list(group['delta'].values)
        return pd.DataFrame(data=[period_delta], columns=[ 
                'delta_2000_2004', 'delta_2005_2009', 'delta_2010_2014', 'delta_2015_2019', 'delta_2017_2021'])
        
    df_delta = delta_0.groupby(by='adm0_code').apply(rearrange_by_period).reset_index().set_index('adm0_code').drop(columns=['level_1'])
    return df_delta

def count_by_peroid(group):
    adm0_code = group.index[0]
    decision = group.groupby(by='start_year')['decision']
    pos = decision.aggregate(lambda x: (x == 1).sum())
    neg = decision.aggregate(lambda x: (x == -1).sum())
    total_basins = decision.get_group(2017).shape[0]

    count = [adm0_code]
    for start_year in pos.index:
        count += [pos.loc[start_year], neg.loc[start_year]]
    count.append(total_basins)

    return pd.DataFrame(data=[count], columns=['ADM0_CODE', 
            'count_basins_plus_2000_2004', 'count_basins_negative_2000_2004',
            'count_basins_plus_2005_2009', 'count_basins_negative_2005_2009',
            'count_basins_plus_2010_2014', 'count_basins_negative_2010_2014',
            'count_basins_plus_2015_2019', 'count_basins_negative_2015_2019',
            'count_basins_plus_2017_2021', 'count_basins_negative_2017_2021',
            'total_basins'
        ])


input_dir = Path("outputs_utest_V1_decision")

import geopandas as gpd
gdf = gpd.read_file("data\hydrobasin_6\hydrobasin_6.shp")
masked_basins = gpd.read_file("data\Masked__basins\SNow_Arid_Mask.shp")

# country name
country_name = pd.read_csv("data\Permanent_water\gaul_0_ts.csv")
country_name = country_name[['adm0_code', 'adm0_name']].set_index('adm0_code').drop_duplicates()



#%% For Debug Only
# folder = 'Permanent_water' # Permanent_water, Reservoirs
# area = 'permanent_area' # permanent_area, seasonal_area

# # delta at country level
# df_delta = get_delta_at_basin_level_0(input_dir, folder, area)

# # count changed basins
# # df = pd.read_csv(input_dir / folder / area / "basins_level_6_utest.csv")
# # df['adm0_code'] = df['id_bgl'].transform(lambda x: eval(x.split("_")[-1]))

# utest = pd.read_csv(input_dir / folder / area / "basins_level_6_utest.csv")
# utest['adm0_code'] = utest['id_bgl'].transform(lambda x: eval(x.split("_")[-1]))
# utest['PFAF_ID'] = utest['id_bgl'].transform(lambda x: eval(x.split("_")[0]))

# gdf_join = gdf.merge(utest, on='PFAF_ID', how='right').to_crs('+proj=robin')
# gdf_join = gdf_join[~gdf_join['PFAF_ID'].isin(list(masked_basins['PFAF_ID_6'].unique()))]

# print('PFAF_ID', len(gdf_join['PFAF_ID'].unique()))
# print('id_bgl', len(gdf_join['id_bgl'].unique()))

# # df_count = gdf_join.groupby(by='adm0_code').apply(count_by_peroid).reset_index().set_index('adm0_code').drop(columns=['level_1'])


#%%

for folder in os.listdir(input_dir):
    for area in os.listdir(input_dir / folder):

        save_dir = Path("outputs_utest_V1_decision") / folder / area
        save_dir.mkdir(exist_ok=True, parents=True)

        # delta at country level
        df_delta = get_delta_at_basin_level_0(input_dir, folder, area)

        # count changed basins
        # df = pd.read_csv(input_dir / folder / area / "basins_level_6_utest.csv")
        # df['adm0_code'] = df['id_bgl'].transform(lambda x: eval(x.split("_")[-1]))

        utest = pd.read_csv(input_dir / folder / area / "basins_level_6_utest.csv")
        utest['adm0_code'] = utest['id_bgl'].transform(lambda x: eval(x.split("_")[-1]))
        utest['PFAF_ID'] = utest['id_bgl'].transform(lambda x: eval(x.split("_")[0]))

        gdf_join = gdf.merge(utest, on='PFAF_ID', how='right').to_crs('+proj=robin')
        gdf_join = gdf_join[~gdf_join['PFAF_ID'].isin(list(masked_basins['PFAF_ID_6'].unique()))]

        df_count = gdf_join.groupby(by='adm0_code').apply(count_by_peroid).reset_index().set_index('adm0_code').drop(columns=['level_1'])

        # country name
        country_name = pd.read_csv("data\Permanent_water\gaul_0_ts.csv")
        country_name = country_name[['adm0_code', 'adm0_name']].set_index('adm0_code').drop_duplicates()

        # merge
        df_count = pd.merge(country_name, df_count, how='inner', left_index=True, right_index=True)
        df_count = pd.merge(df_count, df_delta, how='inner', left_index=True, right_index=True)

        df_count = df_count[['adm0_name', 
                        'delta_2000_2004', 'delta_2005_2009', 'delta_2010_2014', 'delta_2015_2019', 'delta_2017_2021',
                        'count_basins_plus_2000_2004', 'count_basins_negative_2000_2004', 'count_basins_plus_2005_2009',
                        'count_basins_negative_2005_2009', 'count_basins_plus_2010_2014',
                        'count_basins_negative_2010_2014', 'count_basins_plus_2015_2019',
                        'count_basins_negative_2015_2019', 'count_basins_plus_2017_2021',
                        'count_basins_negative_2017_2021', 'total_basins']]
        
        df_count.to_excel(f"outputs_tables_V1/{folder}_{area}.xlsx")
        # df_count.to_excel(f"outputs_tables/{folder}_{area}.csv")

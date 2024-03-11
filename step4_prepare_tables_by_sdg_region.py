#%%
import os
import geopandas as gpd
import pandas as pd
from pathlib import Path

def get_delta_at_basin_level_0(url):
    delta_basin_0 = pd.read_csv(url)
    delta_0 = delta_basin_0[['id_bgl', 'start_year', 'delta']].rename(columns={'id_bgl': 'adm0_code'})

    def rearrange_by_period(group): 
        period_delta = list(group['delta'].values)
        return pd.DataFrame(data=[period_delta], columns=[ 
                'delta_2000_2004', 'delta_2005_2009', 'delta_2010_2014', 'delta_2015_2019', 'delta_2017_2021'])
        
    df_delta = delta_0.groupby(by='adm0_code').apply(rearrange_by_period).reset_index().set_index('adm0_code').drop(columns=['level_1'])
    return df_delta


# drop_duplicates by peroid
def drop_duplicates_by_peroid(gdf_join):
    print("\ndrop_duplicates by peroid")
    gdfs_by_peroid = []
    for start_year in list(gdf_join.start_year.unique()):
        print()
        gdf_perid_raw = gdf_join[gdf_join.start_year==start_year]
        print(start_year, gdf_perid_raw.shape)

        # drop all duplicated rows by PFAF_ID, not sure which is correct id
        gdf_peroid = gdf_perid_raw.drop_duplicates(subset='PFAF_ID', keep=False)
        print(start_year, gdf_peroid.shape)

        gdfs_by_peroid.append(gdf_peroid)
    return pd.concat(gdfs_by_peroid)

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


""" Configuration """
input_dir = Path("outputs_decision")
save_dir = input_dir / 'outputs' / 'tables_by_SDG_region'
save_dir.mkdir(exist_ok=True, parents=True) 


gdf = gpd.read_file("data\hydrobasin_6\hydrobasin_6.shp")
masked_basins = gpd.read_file("data\Masked__basins\SNow_Arid_Mask.shp")

# country name
country_name = pd.read_csv("data/SDG_region_link_table.csv", encoding='latin1')


#%%

""" count basins by SDG region """
for folder in ['Permanent_water', 'Reservoirs']:
    for area in ['permanent_area', 'seasonal_area']:

        data_dir = input_dir / folder / area
        print(f"------------------------------------------")
        print(data_dir)

        # delta at country level
        df_delta = get_delta_at_basin_level_0(data_dir / "basins_level_0_utest.csv")

        utest = pd.read_csv(data_dir / "basins_level_6_utest.csv")
        utest['adm0_code'] = utest['id_bgl'].transform(lambda x: eval(x.split("_")[-1]))
        utest['PFAF_ID'] = utest['id_bgl'].transform(lambda x: eval(x.split("_")[0]))

        # merge results with hydrobasin shape file
        gdf_join = gdf.merge(utest, on='PFAF_ID', how='inner')

        # apply basin masking based on data\Masked__basins\SNow_Arid_Mask.shp
        gdf_join = gdf_join[~gdf_join.PFAF_ID.isin(list(masked_basins.PFAF_ID_6.unique()))]

        # drop_duplicates
        gdf_join = drop_duplicates_by_peroid(gdf_join)

        gdf_join = gdf_join.merge(country_name, on='adm0_code', how='right')
        df_count = gdf_join.groupby(by='SDG_region').apply(count_by_peroid).reset_index().set_index('SDG_region').drop(columns=['level_1'])

        df_count = df_count[[
                        # 'delta_2000_2004', 'delta_2005_2009', 'delta_2010_2014', 'delta_2015_2019', 'delta_2017_2021',
                        'count_basins_plus_2000_2004', 'count_basins_negative_2000_2004', 'count_basins_plus_2005_2009',
                        'count_basins_negative_2005_2009', 'count_basins_plus_2010_2014',
                        'count_basins_negative_2010_2014', 'count_basins_plus_2015_2019',
                        'count_basins_negative_2015_2019', 'count_basins_plus_2017_2021',
                        'count_basins_negative_2017_2021', 'total_basins']]
        
        df_count.to_excel(save_dir / f"{folder}_{area}.xlsx")
        # df_count.to_excel(f"outputs_tables/{folder}_{area}.csv")


#%%
import geopandas as gpd
gdf = gpd.read_file("data/UNEP_Hydro/hybas_world_lv06_wmobb_update_20230203.shp")
gdf

#%%

# masked_basins = gpd.read_file("data\Masked__basins\SNow_Arid_Mask.shp").to_crs('+proj=robin')
# masked_basins['id_bgl'] = masked_basins['PFAF_ID_6'].astype('Int32').astype('str') + '_' + masked_basins['ADM0_CODE'].astype('str')
# gdf = gdf[~gdf['id_bgl'].isin(list(masked_basins['id_bgl'].unique()))]


#%%

import ee
ee.Initialize()

hydro = ee.FeatureCollection("projects/nrt-wildfiremonitoring/assets/hybas_world_lv06_wmobb_update_20230203")
worldPop = ee.ImageCollection('WorldPop/GP/100m/pop')                           

def add_basin_pop(f, year): 
  aoi = f.geometry(100)
  pop = (
    worldPop.filterBounds(aoi)
        .filter(ee.Filter.eq('year', year))
        .mosaic()
        .reduceRegion(ee.Reducer.sum(), aoi, 100)
    )
  return f.set(f'pop_{year}', ee.Number(pop.get('population')).round())

for year in range(2020, 2021):
  print(year)
  hydro = hydro.map(lambda f: add_basin_pop(f, year))

#%%

import geemap
gdf = geemap.ee_to_gdf(hydro)
gdf.to_csv("world_pop/basin_6_world_pop_2000_2020.csv")


#%%
import pandas as pd
df = pd.read_csv("world_pop\dask_outputs\partition_8.csv")
df


#%%

df = pd.read_csv("world_pop/basin_6_world_pop_2000_2020.csv")
df['PFAF_ID'] = df.PFAF_ID.astype('Int32')

river_flow = pd.read_csv("//dkcph1-nas02\jupyterhub-exchange\connorchewning\GHM_freshwater_extraction/for_PMs\level_6_sig_change_table.csv", sep=";")
river_flow['sig_neg'] = river_flow.reject_null_2017_to_2021 * river_flow.sig_change
sig_neg = river_flow[river_flow.sig_neg == -1]
sig_neg

#%%

set1 = set(sig_neg.pfaf_id.unique())
set2 = set(df.PFAF_ID.unique())

diff = set1 - set2



#%%

duplicates = results[results.duplicated('PFAF_ID', keep=False)]

summed_df = res.groupby('PFAF_ID').sum().reset_index()

for col in summed_df.columns:
  summed_df = summed_df.rename(columns={col: col[-4:]})


#%%

import pandas as pd

pop = pd.read_csv("world_pop/basin_6_world_pop_2000_2020_V1.csv")
pop1 = pd.read_csv("world_pop/dask_outputs_todo_ids\partition_0.csv")

pop = pd.concat([pop, pop1]).reset_index()

for idx, year in enumerate(range(2000, 2021)):
  col = f"{idx}_pop_{year}"
  pop[col] = pop[col].round()
  pop = pop.rename(columns={col: year})

pop.set_index("id_bgl").to_csv("world_pop/basin_6_world_pop_2000_2020.csv")

#%%
import pandas as pd

filename = "id_bgl_6_turbidity"
pop = pd.read_csv("world_pop/basin_6_world_pop_2000_2020.csv")
trophic = pd.read_csv(f"world_pop/input/{filename}_summary_events.csv")
trophic_2021 = trophic[(trophic.period==2021) & (trophic.number_of_bad_lakes > 0)]

pop_flt = pop[pop.id_bgl.isin(list(trophic_2021.id_bgl_6))]


years = sorted(['2000', '2010', '2011',
       '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2001',
       '2020', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009'])
col_keep = years + ['id_bgl']

pop_flt = pop_flt[col_keep].rename(columns={'id_bgl': 'id_bgl_6'})

pop_flt.set_index('id_bgl_6').to_csv(f"world_pop/{filename}_pop.csv")


#%%
import pandas as pd

dfc = pd.read_csv("//dkcph1-nas02/jupyterhub-exchange/connorchewning/GHM_freshwater_extraction/for_PMs/level_6_sig_change_table.csv", delimiter=";")
dfc['decision'] = dfc.reject_null_2017_to_2021 * dfc.sig_change
dfc_sig_neg = dfc[dfc.decision==-1]
dfc_sig_neg.shape


#%%
import geopandas as gpd
from pathlib import Path

cto_folder = Path("//dkcph1-nas02/jupyterhub-exchange/cto/UNEP/SDG661_2024_report/MIMU_basin_shapes")
perm_water = gpd.read_file(cto_folder / "Basin_map_SDG661_2024_indicators.shp")
neg = perm_water[perm_water['basin6_Per']=='-1']

#%%
neg_pop = pop[pop.PFAF_ID.isin(list(neg.PFAF_ID))]
diff = set(pop.PFAF_ID.unique()) - set(perm_water.PFAF_ID.unique())


#%%
import ee 
ee.Initialize()

# # 'Basin_map_SDG661_2024_indicators' (permanent water)
# filename = 'Basin_map_SDG661_2024_indicators' 
# property = 'basin6_Per'

# 'River_flow_basins' 
filename = 'River_flow_basins' 
property = 'level_6__2'

# # 'River_flow_basins' 
# filename = 'Seasonal_water_basins' 
# property = 'basin6_Per'

keyIdx = 'PFAF_ID'
npartitions = 10

save_dir = Path(f"world_pop/{filename}") 
save_dir.mkdir(exist_ok=True, parents=True)

hydro = ee.FeatureCollection(f"projects/nrt-wildfiremonitoring/assets/{filename}")
obj_list = (hydro.filter(ee.Filter.eq(property, '-1'))
            .aggregate_array(keyIdx)
            .distinct()
            .getInfo()
      )

len(obj_list)


#%%

import dask.dataframe as dd

filename = 'Basin_map_SDG661_2024_indicators'
ddf = dd.read_csv(f"world_pop/{filename}/partition_*.csv", 
                      on_bad_lines='skip', 
                      assume_missing=True) 
pop = ddf.compute()

for idx, year in enumerate(range(2000, 2021)):
  col = f"{idx}_pop_{year}"
  pop[col] = pop[col].round()
  pop = pop.rename(columns={col: year})

pop.set_index("PFAF_ID").to_csv(f"world_pop/pop_{filename}.csv")

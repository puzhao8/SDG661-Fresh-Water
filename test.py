
#%%

from pathlib import Path
import geopandas as gpd
import numpy as np

jupyterHubDir = Path("//DKCPH1-NAS02/jupyterhub-exchange/nick0693/UNEP_Hydro/hydrobasins")

gdf = gpd.read_file(jupyterHubDir / "hybas_world_lv06_wmobb_update_20230203.shp")

gdf['PFAF_ID'] = gdf['PFAF_ID']
gdf['M49Code'] = gdf['M49Code']
gdf['id_bgl'] = gdf['PFAF_ID'].astype('Int32').astype(str) + '_' + gdf['M49Code'].astype('Int32').astype(str)
gdf['id_bgl'] = gdf['id_bgl'].transform(lambda x: x.replace('<NA>', 'NaN'))

# gdf.set_index('id_bgl').to_file("data/UNEP_Hydro/hybas_world_lv06_wmobb_update_20230203.shp")

gdf


#%%

jupyterHubDir = Path("//DKCPH1-NAS02/jupyterhub-exchange/nick0693/UNEP_Hydro/hydrobasins")
gdf_country = gpd.read_file(jupyterHubDir / "Countries_Separated_with_associated_territories_fix.shp")
gdf_country
#%%

df = gpd.read_file("data/UNEP_Hydro/hybas_world_lv06_wmobb_update_20230203.shp")
df


#%%

import pandas as pd
df_decision = pd.read_csv("outputs_decision\Permanent_water\permanent_area/basins_level_6_utest.csv")
df_2017 = df_decision[df_decision.start_year==2017]
df_2017['PFAF_ID'] = df_2017.id_bgl.transform(lambda x: x.split('_')[0])

df_2017


#%%

import pandas as pd

pop = pd.read_csv("world_pop/basin_6_world_pop_2000_2020.csv")
pop

#%%

yearList = [f"{i}" for i in range(2000, 2021)]
cols_req = yearList + ['M49Code', 'PFAF_ID']
pop.set_index('id_bgl')[cols_req].to_csv("world_pop_202412/basin6_worldpop_by_id_bgl.csv", index=True)

#%% Group pop by PFAF_ID

yearList = [f"{i}" for i in range(2000, 2021)]

basin6_pop = pop.groupby("PFAF_ID")[yearList].sum().reset_index().astype(int)
basin6_pop.to_csv("world_pop_202412/basin6_worldpop_by_PFAF_ID.csv", index=False)

#%% Group pop by Country

yearList = [f"{i}" for i in range(2000, 2021)]

basin6_pop = pop.groupby("M49Code")[yearList].sum().reset_index().astype(int)
basin6_pop.to_csv("world_pop_202412/worldpop_by_country.csv", index=False)
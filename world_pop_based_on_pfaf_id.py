
#%%
import ee
import geemap
import dask
import dask.dataframe as dd

import pandas as pd
import json
from pathlib import Path
from retry import retry
from requests.exceptions import HTTPError 

ee.Initialize()

hydro = ee.FeatureCollection("projects/nrt-wildfiremonitoring/assets/hybas_world_lv06_wmobb_update_20230203")
worldPop = ee.ImageCollection('WorldPop/GP/100m/pop')   

# FAO = ee.FeatureCollection("FAO/GAUL_SIMPLIFIED_500m/2015/level0")
# country = FAO.filter(ee.Filter.eq("ADM0_NAME", "Colombia"))

""" Sample time series over a given point """
# point = ee.Geometry.Point([-72.19594365263514, 4.556298580150745])

def fc_to_gdf(fc):
    try:
        df = geemap.ee_to_gdf(fc)
        return df
    except Exception as e:
        print("----> ", e)
        # if str(e) == "User memory limit exceeded.": return 
        # else: return geemap.ee_to_gdf(fc)
        return pd.DataFrame([])

@retry(HTTPError, tries=10, delay=2, max_delay=60)
def get_basin_pop(id_bgl):

    f = hydro.filter(ee.Filter.eq(keyIdx, id_bgl))
    aoi = f.geometry()
    popImgList = ee.List.sequence(2000, 2021).map(
        lambda year: worldPop.filterBounds(aoi)
            .filter(ee.Filter.eq('year', year))
            .mosaic()
            .rename(ee.String('pop_').cat(ee.Number(year).int().format()))
        )
    
    stack = ee.ImageCollection(popImgList).toBands()                                          
    f = stack.reduceRegions(
            reducer=ee.Reducer.sum(), 
            collection=f, 
            scale=100, 
            crs='EPSG:4326', 
            tileScale =4
        )

    # f = f.set(pop.getInfo())
    # f = f.set('id_bgl', id_bgl)
    return fc_to_gdf(f)



#%%
from pathlib import Path
import pandas as pd
import dask_geopandas as ddg
import multiprocessing
multiprocessing.freeze_support()

import io
from dask.distributed import Client, LocalCluster

if __name__ == "__main__":
    
  cluster = LocalCluster(n_workers=8, threads_per_worker=4, dashboard_address=':38787')
  client = Client(cluster) # timeout

  
#   # 'Basin_map_SDG661_2024_indicators' (permanent water)
#   filename = 'Basin_map_SDG661_2024_indicators' 
#   property = 'basin6_Per'

  # 'River_flow_basins' 
  filename = 'River_flow_basins' 
  property = 'level_6__2'

#   # 'River_flow_basins' 
#   filename = 'Seasonal_water_basins' 
#   property = 'level_6_2'

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

#   obj_list = ['811102_124']
#   from input import todo_ids as obj_list
  

  df = pd.DataFrame({keyIdx: obj_list})
  ddf = ddg.from_geopandas(df, npartitions=npartitions)
  print(df)

  def sample_partition(partition, partition_info):
      partition_number = partition_info["number"]
      save_url = save_dir / f"partition_{partition_number}.csv"

      reinit_csv = True
      for row_idx, row in enumerate(partition.itertuples()):
          id_bgl = row.PFAF_ID
          print(f"partition: {partition_number}, {keyIdx}: {id_bgl}")

          df = get_basin_pop(id_bgl)
          # df = df.drop(columns=['geometry'], axis=1)

          if reinit_csv:  
              df.set_index(keyIdx).to_csv(save_url)
              reinit_csv = False

          else: 
              if keyIdx in df.columns:
                  df.set_index(keyIdx).to_csv(save_url, mode='a', header=False, index=True)
              else:
                  pass

  ddf.map_partitions(sample_partition, meta=(None, object)).compute()


#%%

# import dask.dataframe as dd

# filename = 'Basin_map_SDG661_2024_indicators'
# ddf = dd.read_csv(f"world_pop/{filename}/partition_*.csv", 
#                       on_bad_lines='skip', 
#                       assume_missing=True) 
# pop = ddf.compute()

# for idx, year in enumerate(range(2000, 2021)):
#   col = f"{idx}_pop_{year}"
#   pop[col] = pop[col].round()
#   pop = pop.rename(columns={col: year})

# pop.set_index("PFAF_ID").to_csv(f"world_pop/pop_{filename}.csv")
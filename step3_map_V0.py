
#%% Delta
import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
import os, warnings

""" configuration """
folder = 'Permanent_water' # Permanent_water, Reservoirs
area = 'permanent_area' # permanent_area, seasonal_area
start_year = 2017
p_thd = 0.025
basin_level = 6 # basin level
save_flag = True 
csv_init_flag = True # csv initalization flag

legend_extend = True # extend legend to include the number of total basins, dry basins, and mask basins
input_dir = Path("outputs_decision/updated_decision")

if legend_extend:   
    maps_dir = input_dir / 'outputs' / 'maps_V0_legend_extend'
else:
    maps_dir = input_dir / 'outputs' / 'maps_V0'

# os.system(f"del {maps_dir}")
maps_dir.mkdir(exist_ok=True, parents=True) 

gdf = gpd.read_file("data\hydrobasin_6\hydrobasin_6.shp")
# gdf = gpd.read_file("data/UNEP_Hydro/hybas_world_lv06_wmobb_update_20230203.shp")

MERGE_COL = 'PFAF_ID'

# %%
for folder in ['Permanent_water', 'Reservoirs']:
    for area in ['permanent_area', 'seasonal_area']:

# for folder in ['Permanent_water']:
#     for area in ['seasonal_area']:

        save_dir = maps_dir / folder / area
        save_dir.mkdir(exist_ok=True, parents=True)

        if folder == 'Permanent_water': alpha = 1.5 # mean +/- alpha * std
        if folder == 'Reservoirs': alpha = 1.0 # mean +/- alpha * std
        if 'seasonal_area' == area: alpha = 2

        utest_all = pd.read_csv(input_dir / folder / area / f"basins_level_{basin_level}_utest.csv")
        utest = utest_all[utest_all.start_year== start_year]

        utest['PFAF_ID'] = utest['id_bgl'].transform(lambda x: eval(x.split("_")[0]))

        #%%
        print()
        print(f"----------------------- alpha: {alpha}, p_thd: {p_thd} --------------------------------------")

        num_before_basin_masking = utest.shape[0]
        print(f"before applying masked_basins: {num_before_basin_masking}")

        masked_basins = gpd.read_file("data\Masked__basins\SNow_Arid_Mask.shp")
        masked_basins['PFAF_ID_6'] = masked_basins['PFAF_ID_6'].transform(lambda x: eval(x))


        #%% Map Product

        """ Global Map: Delta vs. U-Test """
        import geopandas as gpd
        import matplotlib.pyplot as plt

        import matplotlib.patches as mpatches
        from pathlib import Path

        # from cartopy import crs as ccrs
        # robinson = ccrs.Robinson().proj4_init

        from matplotlib.colors import LinearSegmentedColormap
        colors = ['orange', '#fefefe', '#1560bd']
        my_colormap = LinearSegmentedColormap.from_list("my_colormap", colors)

        def get_handles(stats_dict, legend_extend=False):
            handles = []
            for idx, key in enumerate(['Decrease', 'Neutral', 'Increase']):
                handle = mpatches.Patch(color=colors[idx], edgecolor="gray", linewidth=0.2, label=f'{key} (n={stats_dict[key]})')
                handles.append(handle)

            if legend_extend:
                for idx, key in enumerate(['total', 'dry', 'masked']):
                    handle = mpatches.Patch(color='gray', edgecolor="gray", linewidth=0.2, label=f'{key} (n={stats_dict[key]})')
                    handles.append(handle)
            return handles


        countries = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        # countries_filtered = countries[countries.geometry.apply(lambda x: x.bounds[0] > -100)]
        countries_flt = countries[countries.geometry.apply(lambda x: x.bounds[1] > -60)].to_crs('+proj=robin')

        """ _dissolve: PFAF_ID_6 + Country ID """        
        gdf_join = gdf[['PFAF_ID', 'geometry']].merge(utest, on='PFAF_ID', how='right').to_crs('+proj=robin')

        """ mask dry basins (PFAF_ID is int) """
        masked_basins = gpd.read_file("data\Masked__basins\SNow_Arid_Mask.shp").to_crs('+proj=robin')
        masked_basins['PFAF_ID'] = masked_basins['PFAF_ID_6'].astype('Int32') #.astype('str') #+ '_' + masked_basins['ADM0_CODE'].astype('str')

        num_before_basin_masking = gdf_join.shape[0]

        # apply basin masking based on data\Masked__basins\SNow_Arid_Mask.shp
        if True:
            gdf_join = gdf_join[~gdf_join['PFAF_ID'].isin(list(masked_basins['PFAF_ID'].unique()))]

        print(f"after applying masked_basins: {gdf_join.shape[0]}")
        num_of_masked_basins = num_before_basin_masking - len(gdf_join.PFAF_ID.unique())
        print(f"number of masked basins: {num_of_masked_basins}")

        # drop duplicates by column PFAF_ID
        gdf_join = gdf_join.drop_duplicates(subset='PFAF_ID', keep=False) # newly added

        thd_low = gdf_join.thd_low.iloc[0]
        thd_high = gdf_join.thd_high.iloc[0]

        total_basins = len(gdf_join.PFAF_ID.unique()) 
        neg = gdf_join[gdf_join.decision == -1].shape[0]
        stable = gdf_join[(gdf_join.decision == 0) | (gdf_join.decision == -99)].shape[0]
        pos = gdf_join[gdf_join.decision == 1].shape[0]
        dry_basins = gdf_join[gdf_join.decision == -99].shape[0]

        stats_dict = {
            'Decrease': neg,
            'Neutral': stable,
            'Increase': pos,

            'total': total_basins,
            'dry': dry_basins,
            'masked': num_of_masked_basins
        }


        print()
        print(f"neg: {neg}, stable: {stable}, pos: {pos}, dry_basins: {dry_basins}, masked_basins: {num_of_masked_basins}, total_basins: {total_basins}")

        #%%
        """ plot figures """
        fig, ax = plt.subplots(figsize=(12, 6))
        gdf_join.loc[gdf_join.decision == -99, 'decision'] = 0 # map -99 into 0

        """ save for MIMU """
        MIMU_csv_dir = maps_dir / "MIMU_csv"
        MIMU_csv_dir.mkdir(exist_ok=True, parents=True)
        gdf_join[['id_bgl', 'PFAF_ID', 'decision']].set_index('id_bgl').to_csv(MIMU_csv_dir / f"basin{basin_level}_{folder}_{area}.csv")

        """ plot global map """
        gdf_join.plot(ax=ax, column='decision', cmap=my_colormap, vmin=-1, vmax=1)
        countries_flt.plot(ax=ax, color='none', edgecolor='black', linewidth=0.5)
        # masked_basins.plot(ax=ax, color='white', edgecolor='white', linewidth=0.5)

        title = f"{folder}/{area} ({start_year}): u_test (p={p_thd:.3f}, with delta mask) \n masked basins ({alpha} std) where {thd_low:.2f}% <= delta <= {thd_high:.2f}% or baseline < 0.025 sq km"
        if not legend_extend: 
            title = title.split(":")[0]

        save_url = maps_dir / f'basin{basin_level}_{folder}_{area}_{start_year}_a_{alpha:.1f}_p_{p_thd:.3f}.png'

        ax.set_title(title)
        ax.legend(handles=get_handles(stats_dict=stats_dict, legend_extend=legend_extend), loc='lower left') #

        # Remove the plot box by hiding the spines
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.set_xticks([])
        ax.set_yticks([])
        plt.tight_layout()

        print(save_url)
        if save_flag: fig.savefig(save_url, dpi=300)
        print()

        row_single = [folder, area, basin_level, start_year, alpha, p_thd, 
                      thd_low, thd_high, 'delta + utest', neg, stable, pos, total_basins,
                      dry_basins, num_of_masked_basins]

        df = pd.DataFrame([row_single], columns=[
                            'folder', 'area', 'basin_level', 'start_year', 'alpha', 'p_thd',
                            'thd_low', 'thd_high', 'method', 'neg', 'stable', 'pos', 'total_basins',
                            'dry_basins', 'num_of_masked_basins'])
        
        url = maps_dir / f'stats.csv'
        if csv_init_flag:
            if url.exists(): 
                os.system(f"del {url}")
                print(f"del existed: {url}")

            df.to_csv(url, mode='w')
            csv_init_flag = False

        else: 
            df.to_csv(url, mode='a', header=False)

        # df.to_excel(maps_dir / f"{folder}_{area}.xlsx")

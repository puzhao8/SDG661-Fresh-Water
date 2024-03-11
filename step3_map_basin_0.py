
#%% Delta
import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

""" configuration """
folder = 'Permanent_water' # Permanent_water, Reservoirs
area = 'permanent_area' # permanent_area, seasonal_area
basin_level = 0 # basin level
start_year = 2017
p_thd = 0.025
save_flag = True 

legend_extend = False # extend legend to include the number of total basins, dry basins, and mask basins
input_dir = Path("outputs_decision")

if legend_extend:   
    maps_dir = input_dir / 'outputs' / 'maps_legend_extend'
else:
    maps_dir = input_dir / 'outputs' / 'maps'
maps_dir.mkdir(exist_ok=True, parents=True) 


for folder in ['Permanent_water', 'Reservoirs']:
    for area in ['permanent_area', 'seasonal_area']:

        if folder == 'Permanent_water': alpha = 1.5 # mean +/- alpha * std
        if folder == 'Reservoirs': alpha = 1.0 # mean +/- alpha * std

        save_dir = maps_dir / folder / area
        save_dir.mkdir(exist_ok=True, parents=True)

        utest_all = pd.read_csv(input_dir / folder / area / f"basins_level_{basin_level}_utest.csv")
        utest = utest_all[utest_all['start_year'] == start_year]


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


        gdf = gpd.read_file("data\Countries_Separated_with_associated_territories_fix\Countries_Separated_with_associated_territories_fix.shp")

        countries = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        # countries_filtered = countries[countries.geometry.apply(lambda x: x.bounds[0] > -100)]
        countries_flt = countries[countries.geometry.apply(lambda x: x.bounds[1] > -60)]#.to_crs('+proj=robin')

        """ _dissolve: PFAF_ID_6 + Country ID """
        utest = utest.rename(columns={'id_bgl': 'M49Code'})
        utest['M49Code'] = utest['M49Code'].transform(lambda x: str(x))

        gdf_join = gdf.merge(utest, on='M49Code', how='inner')#.to_crs('+proj=robin')
        num_of_masked_basins = -99

        #%%

        # drop duplicates by column PFAF_ID
        gdf_join = gdf_join.drop_duplicates(subset='M49Code', keep=False) # newly added

        thd_low = gdf_join.thd_low.iloc[0]
        thd_high = gdf_join.thd_high.iloc[0]

        total = len(gdf_join['M49Code'].unique()) 
        neg = gdf_join[gdf_join.decision == -1].shape[0]
        stable = gdf_join[(gdf_join.decision == 0) | (gdf_join.decision == -99)].shape[0]
        pos = gdf_join[gdf_join.decision == 1].shape[0]
        dry_basins = gdf_join[gdf_join.decision == -99].shape[0]

        stats_dict = {
            'Decrease': neg,
            'Neutral': stable,
            'Increase': pos,

            'total': total,
            'dry': dry_basins,
            'masked': num_of_masked_basins
        }


        print()
        print(f"neg: {neg}, stable: {stable}, pos: {pos}, dry_basins: {dry_basins}, masked_basins: {num_of_masked_basins}, total_basins: {total}")

        #%%
        """ plot figure """
        fig, ax = plt.subplots(figsize=(12, 6))
        gdf_join.loc[gdf_join['decision'] == -99, 'decision'] = 0 
        gdf_join.plot(ax=ax, column='decision', cmap=my_colormap, vmin=-1, vmax=1)
        countries_flt.plot(ax=ax, color='none', edgecolor='black', linewidth=0.5)

        title = f"{folder}/{area} ({start_year}): u_test (p={p_thd:.3f}, with delta mask) \n masked basins ({alpha} std) where {thd_low:.2f}% <= delta <= {thd_high:.2f}% or baseline < 0.025 sq km"
        save_url = save_dir / f'basin{basin_level}_{start_year}_a_{alpha:.1f}_p_{p_thd:.3f}.png'

        plt.tight_layout()
        ax.set_title(title)
        ax.legend(handles=get_handles(stats_dict=stats_dict, legend_extend=legend_extend), loc='lower left') #

        # Remove the plot box by hiding the spines
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.set_xticks([])
        ax.set_yticks([])
        print(save_url)
        if save_flag: fig.savefig(save_url, dpi=300)
        print()

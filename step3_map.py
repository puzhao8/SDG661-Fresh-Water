
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
start_year = 2015
p_thd = 0.025
save_flag = True 

basin_level = 6 # basin level
if folder == 'Permanent_water': alpha = 1.5 # mean +/- alpha * std
if folder == 'Reservoirs': alpha = 1.0 # mean +/- alpha * std

maps_dir = Path('maps_V1') 
save_dir = maps_dir / folder / area
save_dir.mkdir(exist_ok=True, parents=True)

df = pd.DataFrame([], columns=['folder', 'area', 'basin_level', 'alpha', 'p_thd', 'num_of_masked_basins', 
                                        'thd_low', 'thd_high', 'method', 'neg', 'stable', 'pos'])
df.to_csv(f'maps/{folder}_{area}.csv', mode='w')

# for alpha in [1, 1.5, 2, 2.5, 3]:
#     for p_thd in [0.01, 0.02, 0.025, 0.05]:



input_dir = Path("outputs_utest_V1_decision")
utest_all = pd.read_csv(input_dir / folder / area / f"basins_level_{basin_level}_utest.csv")
utest = utest_all[utest_all['start_year'] == start_year]

utest['PFAF_ID'] = utest['id_bgl'].transform(lambda x: eval(x.split("_")[0]))


#%%
print()
print(f"----------------------- alpha: {alpha}, p_thd: {p_thd} --------------------------------------")

num_before_basin_masking = utest.shape[0]
print(f"before applying masked_basins: {num_before_basin_masking}")

masked_basins = gpd.read_file("data\Masked__basins\SNow_Arid_Mask.shp")
masked_basins['PFAF_ID_6'] = masked_basins['PFAF_ID_6'].transform(lambda x: eval(x))
utest = utest[~utest['PFAF_ID'].isin(masked_basins['PFAF_ID_6'].unique())]
# utest

print(f"after applying masked_basins: {utest.shape[0]}")
num_of_masked_basins = num_before_basin_masking - utest.shape[0]
print(f"number of masked basins: {num_of_masked_basins}")


#%% Map Product

""" Global Map: Delta vs. U-Test """
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.patches as mpatches
from pathlib import Path

# from cartopy import crs as ccrs
# robinson = ccrs.Robinson().proj4_init
colors = ['orange', '#fefefe', '#1560bd']

def get_handles(count=[]):
    decrease_handle = mpatches.Patch(color=colors[0], edgecolor="gray", linewidth=0.2, label=f'Decrease (n={count[0]})')
    no_change_handle = mpatches.Patch(color=colors[1], edgecolor="gray", linewidth=0.2, label=f'Neutral (n={count[1]})')
    increase_handle = mpatches.Patch(color=colors[2], edgecolor="gray", linewidth=0.2, label=f'Increase (n={count[2]})')
    dry_basins_handle = mpatches.Patch(color=colors[1], edgecolor="gray", linewidth=0.2, label=f'Dry Basin (n={count[3]})')
    masked_basins_handle = mpatches.Patch(color=colors[1], edgecolor="gray", linewidth=0.2, label=f'Masked Basin (n={count[4]})')
    return [decrease_handle, no_change_handle, increase_handle, dry_basins_handle, masked_basins_handle]
    # ax.legend(handles=[decrease_handle, no_change_handle, increase_handle])


gdf = gpd.read_file("data\hydrobasin_6\hydrobasin_6.shp")
my_colormap = LinearSegmentedColormap.from_list("my_colormap", colors)

countries = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
# countries_filtered = countries[countries.geometry.apply(lambda x: x.bounds[0] > -100)]
countries_flt = countries[countries.geometry.apply(lambda x: x.bounds[1] > -60)].to_crs('+proj=robin')

""" _dissolve: PFAF_ID_6 + Country ID """
gdf_join = gdf.merge(utest, on='PFAF_ID', how='right').to_crs('+proj=robin')

masked_basins = gpd.read_file("data\Masked__basins\SNow_Arid_Mask.shp").to_crs('+proj=robin')
gdf_join = gdf_join[~gdf_join['PFAF_ID'].isin(list(masked_basins['PFAF_ID_6'].unique()))]
gdf_join[gdf_join['decision'] == -99]['decsion'] = 0 


thd_low = utest['thd_low'].iloc[0]
thd_high = utest['thd_high'].iloc[0]

col = 'decision'
neg = utest[utest[col] == -1].shape[0]
stable = utest[utest[col] == 0].shape[0]
dry_basins = utest[utest[col] == -99].shape[0]
pos = utest[utest[col] == 1].shape[0]
row_single = [folder, area, basin_level, alpha, p_thd, num_of_masked_basins, thd_low, thd_high, 'utest', neg, stable, pos, dry_basins]

fig, ax = plt.subplots(figsize=(12, 6))
gdf_join.plot(ax=ax, column=col, cmap=my_colormap, vmin=-1, vmax=1)
# gdf_join.to_crs('+proj=eck4').plot(ax=ax, column='permanent_area', cmap=my_colormap, vmin=-100, vmax=100)
# masked_basins.plot(ax=ax, color='white', edgecolor='white', linewidth=0.5)
countries_flt.plot(ax=ax, color='none', edgecolor='black', linewidth=0.5)

print()
print(f"neg: {neg}, stable: {stable}, pos: {pos}, dry_basins: {dry_basins}, masked_basins: {num_of_masked_basins}")

# title = f'{folder}/{area}: delta ({alpha:.1f} std: {thd_low:.2f}% <= delta <= {thd_high:.2f}%)'
# save_url = maps_dir / f'delta_a_{alpha:.1f}.png'

title = f"{folder}/{area}: u_test (p={p_thd:.3f}, with delta mask) \n masked basins ({alpha} std) where {thd_low:.2f}% <= delta <= {thd_high:.2f}% or baseline < 0.025 sq km"
save_url = save_dir / f'utest_a_{alpha:.1f}_p_{p_thd:.3f}_{start_year}.png'


plt.tight_layout()
ax.set_title(title)
ax.legend(handles=get_handles([neg, stable, pos, dry_basins, num_of_masked_basins]), loc='lower left') #

# Remove the plot box by hiding the spines
for spine in ax.spines.values():
    spine.set_visible(False)
ax.set_xticks([])
ax.set_yticks([])
print(save_url)
if save_flag: fig.savefig(save_url, dpi=300)
print()


df = pd.DataFrame([row_single], columns=['folder', 'area', 'basin_level', 'alpha', 'p_thd', 'num_of_masked_basins', 
                            'thd_low', 'thd_high', 'method', 'neg', 'stable', 'pos', 'dry_basins'])
# df.to_csv(maps_dir / f"{folder}_{area}.csv", mode='a', header=False)
df.to_excel(maps_dir / f"{folder}_{area}.xlsx")

#%% Delta
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

""" configuration """
folder = 'Permanent_water' # Permanent_water, Reservoirs
area = 'permanent_area' # permanent_area, seasonal_area

basin_level = 6 # basin level
alpha = 3 # mean +/- alpha * std
p_thd = 0.05
save_flag = True 


""" delta """
delta = pd.read_csv(f"outputs_delta/{folder}/{area}/basins_level_{basin_level}_ts_delta.csv")
delta = delta[delta['start_year']==2017]

delta['PFAF_ID'] = delta['id_bgl'].transform(lambda id: eval(id.split("_")[0]))
delta['Country_ID'] = delta['id_bgl'].transform(lambda id: eval(id.split("_")[-1]))
# delta = delta[(delta['Country_ID'] == Country_ID)]  
# df.set_index('PFAF_ID').dropna().to_csv(f'outputs_map/{folder}_basins_level_6_delta.csv')

""" delta thresholds """
df_thd = pd.read_csv(f"outputs_delta/{folder}/{area}/basin_level_mean_std.csv").set_index('basin_level')
mean = df_thd[f'mean_{area}'].iloc[basin_level]
std = df_thd[f'std_{area}'].iloc[basin_level]

thd_low = mean - alpha * std 
thd_high = mean + alpha * std 
print(f"thd_low: {thd_low}, thd_high: {thd_high}")

def get_sign(x):
    if x < thd_low: return -1.0
    if x > thd_high: return 1.0
    return 0.0

delta['sign'] = delta[area].transform(get_sign)

#%% U-TEST

# p_thd = 0.05
u_test = pd.read_csv(f"outputs_utest/{folder}/{area}/basins_level_{basin_level}_utest.csv")
u_test['u_sign'] = u_test['p_u'].transform(lambda x: float(x < p_thd))
if 'id' in u_test.columns: u_test = u_test.rename(columns={'id': 'id_bgl'})

u_test['PFAF_ID'] = u_test['id_bgl'].transform(lambda id: eval(id.split("_")[0]))
u_test['u_sign'] = u_test['u_score'].transform(lambda x:  np.round(x / (abs(x) + 1e-3))) * u_test['u_sign']
u_test = u_test.rename(columns={'id': 'id_bgl'})
# df.set_index('PFAF_ID').dropna().to_csv(f'outputs_map/{folder}/basins_level_6_utest_p_thd_0_01.csv')
# u_test

#%% Merge DataFrame

""" merge delta and utest """
delta_u = pd.merge(delta, u_test, on='id_bgl')
delta_u['u_sign'][delta_u['sign']==0] = 0 # if delta_sign = 0, u_sign = 0
delta_u['u_sign'] = delta_u['u_sign'].transform(lambda x: x if np.abs(x) > 0 else 0)
delta_u = delta_u.rename(columns={'PFAF_ID_x': 'PFAF_ID'})
# delta_u


#%% Map Product

""" Global Map: Delta vs. U-Test """
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.patches as mpatches
from pathlib import Path

# from cartopy import crs as ccrs
# robinson = ccrs.Robinson().proj4_init

def get_handles(count=[], colors = ['orange', '#d3d3d3', '#1560bd']):
    decrease_handle = mpatches.Patch(color=colors[0], edgecolor="gray", linewidth=0.2, label=f'Decrease (n={count[0]})')
    no_change_handle = mpatches.Patch(color=colors[1], edgecolor="gray", linewidth=0.2, label=f'Neutral (n={count[1]})')
    increase_handle = mpatches.Patch(color=colors[2], edgecolor="gray", linewidth=0.2, label=f'Increase (n={count[2]})')
    return [decrease_handle, no_change_handle, increase_handle]
    # ax.legend(handles=[decrease_handle, no_change_handle, increase_handle])


gdf = gpd.read_file("data\hydrobasin_6\hydrobasin_6.shp")

colors = ['orange', 'white', '#1560bd']
my_colormap = LinearSegmentedColormap.from_list("my_colormap", colors)

countries = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
# countries_filtered = countries[countries.geometry.apply(lambda x: x.bounds[0] > -100)]
countries_flt = countries[countries.geometry.apply(lambda x: x.bounds[1] > -60)].to_crs('+proj=robin')

""" _dissolve: PFAF_ID_6 + Country ID """
masked_basins = gpd.read_file("data\Masked__basins\SNow_Arid_Mask.shp").to_crs('+proj=robin')
gdf_join = gdf.merge(delta_u, on='PFAF_ID', how='right').to_crs('+proj=robin')
gdf_join['PFAF_ID'] = gdf_join['PFAF_ID'].transform(lambda x: str(x))
gdf_join = gdf_join[~gdf_join['PFAF_ID'].isin(list(masked_basins['PFAF_ID_6'].unique()))]

maps_dir = Path('maps') / folder / area
maps_dir.mkdir(exist_ok=True, parents=True)

for col in ['sign', 'u_sign']: # 'sign', 'u_sign'
    fig, ax = plt.subplots(figsize=(12, 5))
    gdf_join.plot(ax=ax, column=col, cmap=my_colormap, vmin=-1, vmax=1)
    # gdf_join.to_crs('+proj=eck4').plot(ax=ax, column='permanent_area', cmap=my_colormap, vmin=-100, vmax=100)
    # masked_basins.plot(ax=ax, color='white', edgecolor='white', linewidth=0.5)
    countries_flt.plot(ax=ax, color='none', edgecolor='black', linewidth=0.5)

    neg = gdf_join[gdf_join[col] < 0].shape[0]
    stable = gdf_join[gdf_join[col] == 0].shape[0]
    pos = gdf_join[gdf_join[col] > 0].shape[0]
    print(f"neg: {neg}, stable: {stable}, pos: {pos}")

    if 'sign' == col: # delta
        title = f'{folder}/{area}: delta (with {alpha} std thresholds)'
        save_url = maps_dir / f'delta_alpha_{alpha}.png'
        
    if 'u_sign' == col: # utest
        title = f'{folder}/{area}: u_test (p={p_thd:.2f}, masked basins where delta = 0)'
        save_url = maps_dir / f'utest_p_{p_thd:.2f}.png'

    plt.tight_layout()
    ax.set_title(title)
    ax.legend(handles=get_handles([neg, stable, pos]), loc='lower left') #
    
    # # Remove the plot box by hiding the spines
    # for spine in ax.spines.values():
    #     spine.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])
    print(save_url)
    if save_flag: fig.savefig(save_url, dpi=300)
    print()
    


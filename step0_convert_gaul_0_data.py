import pandas as pd

for folder in  ['Pemanent_water', 'Reservoirs']:
    df = pd.read_csv(f"data/{folder}/gaul_0_ts.csv")
    df = df.rename(columns={'adm0_code': 'id_bgl_0'}).drop(columns=['adm0_name']).set_index('id_bgl_0')
    df.to_csv(f"data/{folder}/basins_level_0_ts.csv")
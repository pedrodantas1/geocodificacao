import pandas as pd
import numpy as np

df = pd.read_csv("coordenadas_aracaju_google_maps.csv")

df["raio_cobertura_km"] = np.random.randint(3, 6, size=len(df))

# salva de volta
df.to_csv("coordernadas_aracaju_com_area.csv", index=False)

import os

import pandas as pd

# ===== CONFIGURAÇÕES =====
arquivo_principal = "Dim_Geografia.csv"
arquivo_coordenadas = "coordenadas_uf.csv"
arquivo_saida = "ufs_com_lat_long.csv"

PASTA_TRATADOS = "Dados_Tratados"
PASTA_AUXILIARES = "scripts_auxiliares"

# ===== LER CSVs =====
df_principal = pd.read_csv(
    os.path.join(PASTA_TRATADOS, arquivo_principal), sep=";", encoding="utf-8"
)
df_coords = pd.read_csv(
    os.path.join(PASTA_AUXILIARES, arquivo_coordenadas), sep=";", encoding="utf-8"
)

# Padronizar nomes das colunas (caso venham com aspas)
df_coords.columns = df_coords.columns.str.replace('"', "").str.strip()

# ===== RENOMEAR LAT/LONG DO MUNICÍPIO =====
df_principal.rename(
    columns={"Latitude": "Latitude_Municipio", "Longitude": "Longitude_Municipio"},
    inplace=True,
)

# ===== RENOMEAR LAT/LONG DA UF =====
df_coords.rename(
    columns={"latitude": "Latitude_UF", "longitude": "Longitude_UF"}, inplace=True
)

# Padronizar UF
df_principal["UF"] = df_principal["UF"].str.strip().str.upper()
df_coords["uf"] = df_coords["uf"].str.strip().str.upper()

# ===== MERGE =====
df_final = df_principal.merge(df_coords, left_on="UF", right_on="uf", how="left")

# Remover coluna auxiliar "uf"
df_final.drop(columns=["uf"], inplace=True)

# ===== SALVAR RESULTADO =====
df_final.to_csv(arquivo_saida, sep=";", index=False, encoding="utf-8")

print("Processo concluído! Arquivo salvo como:", arquivo_saida)

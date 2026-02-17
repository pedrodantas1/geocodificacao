import os

import pandas as pd

# Caminho do arquivo
ARQUIVO = os.path.join("Dados_Tratados", "Dim_Unidades_Saude.csv")


def contar_lat_long_faltantes():
    print("--- 📊 RELATÓRIO DE PENDÊNCIAS DE GEOLOCALIZAÇÃO ---")

    if not os.path.exists(ARQUIVO):
        print(f"❌ Arquivo não encontrado: {ARQUIVO}")
        return

    try:
        # Lê o arquivo como texto para garantir que '0' ou 'None' sejam lidos corretamente
        df = pd.read_csv(ARQUIVO, sep=";", dtype=str)

        total_registros = len(df)

        # Critério de 'Sem Localização':
        # 1. É Nulo (NaN)
        # 2. É Vazio ('')
        # 3. É a string 'None' ou 'nan'
        # 4. É '0'
        mask_faltante = (
            (df["Latitude"].isna())
            | (df["Latitude"] == "")
            | (df["Latitude"].astype(str).str.lower() == "none")
            | (df["Latitude"].astype(str).str.lower() == "nan")
            | (df["Latitude"] == "0")
        )

        qtd_faltante = mask_faltante.sum()
        qtd_preenchido = total_registros - qtd_faltante
        percentual = (qtd_faltante / total_registros) * 100

        print(f"Total de Unidades:      {total_registros}")
        print(f"✅ Com Latitude/Long:   {qtd_preenchido}")
        print(f"⚠️  Sem Latitude/Long:   {qtd_faltante}")
        print(f"📉 Percentual Pendente: {percentual:.2f}%")

    except Exception as e:
        print(f"❌ Erro ao ler o arquivo: {e}")


if __name__ == "__main__":
    contar_lat_long_faltantes()

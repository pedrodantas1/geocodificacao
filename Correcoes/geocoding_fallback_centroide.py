import os

import geopandas as gpd
import pandas as pd

# ============================
# CONFIGURAÇÕES
# ============================
PASTA_TRATADOS = "Dados_Tratados"
PASTA_TRABALHO = "Correcoes"
ARQUIVO_PRINCIPAL = os.path.join(PASTA_TRATADOS, "Dim_Unidades_Saude.csv")
ARQUIVO_FALHAS = os.path.join(PASTA_TRABALHO, "cnes_nao_encontrados.csv")
ARQUIVO_MALHA_LOCAL = "malha_ibge_brasil.json"
ARQUIVO_SAIDA_CENTROIDE = os.path.join(
    PASTA_TRABALHO, "novas_coordenadas_fallback_centroides.csv"
)


def gerar_centroides_fallback():
    print("--- 🎯 GERANDO CENTROIDES MUNICIPAIS (FALLBACK IBGE) ---")

    if not os.path.exists(ARQUIVO_FALHAS) or not os.path.exists(ARQUIVO_MALHA_LOCAL):
        print("❌ Arquivo de falhas ou malha do IBGE não encontrados.")
        return

    # 1. Lendo as falhas e a base principal para garantir que temos o ID_Municipio
    print("   Lendo registros não encontrados...")
    df_falhas = pd.read_csv(ARQUIVO_FALHAS, sep=";", dtype=str)
    df_main = pd.read_csv(ARQUIVO_PRINCIPAL, sep=";", dtype=str)

    # Garante que temos as informações mais puras do original
    df_pendentes = df_main[df_main["CNES"].isin(df_falhas["CNES"])].copy()

    if df_pendentes.empty:
        print("✅ Nenhum registro para processar.")
        return

    # 2. Lendo a Malha do IBGE
    print("   Carregando o mapa vetorial do IBGE...")
    gdf_ibge = gpd.read_file(ARQUIVO_MALHA_LOCAL)

    # 3. Calculando os Centroides Matemáticos
    print("   Calculando o centro geométrico de cada município do Brasil...")
    # Para evitar avisos de precisão do geopandas, projetamos para Pseudo-Mercator (metros),
    # calculamos o centroide e voltamos para Lat/Long (EPSG:4326)
    gdf_ibge["centroide"] = gdf_ibge.to_crs(epsg=3857).centroid.to_crs(epsg=4326)

    # Extrai a Latitude (y) e Longitude (x) do ponto central
    gdf_ibge["Centroide_Lat"] = gdf_ibge["centroide"].y.astype(str)
    gdf_ibge["Centroide_Lng"] = gdf_ibge["centroide"].x.astype(str)

    # Cria um dicionário rápido para buscar: { '1234567': ('lat', 'long') }
    dict_centroides = {}
    for _, row in gdf_ibge.iterrows():
        cod_area = str(row.get("codarea", ""))
        if cod_area:
            dict_centroides[cod_area] = (row["Centroide_Lat"], row["Centroide_Lng"])

    # 4. Atribuindo as coordenadas aos postos de saúde
    print("   Atribuindo coordenadas aos postos sem endereço exato...")
    novos_achados = []

    for _, row in df_pendentes.iterrows():
        id_mun_csv = str(row.get("ID_Municipio", ""))[:6]  # Pega os 6 primeiros dígitos

        # Procura no dicionário um código do IBGE que comece com esses 6 dígitos
        lat_found, long_found = None, None
        for cod_ibge, coords in dict_centroides.items():
            if cod_ibge.startswith(id_mun_csv):
                lat_found, long_found = coords
                break

        if lat_found and long_found:
            novos_achados.append(
                {
                    "CNES": row["CNES"],
                    "Latitude_Nova": lat_found,
                    "Longitude_Nova": long_found,
                    "Qualidade_Busca": "CENTROIDE_IBGE",  # Flag para você saber no BI
                    "Endereco_Google": "Centro Geográfico do Município (Fallback)",
                }
            )

    # 5. Salvando o arquivo Delta
    qtd_salvos = len(novos_achados)
    if qtd_salvos > 0:
        df_saida = pd.DataFrame(novos_achados)
        df_saida.to_csv(ARQUIVO_SAIDA_CENTROIDE, sep=";", index=False)
        print(
            f"\n✅ Concluído! {qtd_salvos} coordenadas de centroides geradas e salvas em: {ARQUIVO_SAIDA_CENTROIDE}"
        )
    else:
        print(
            "\n⚠️ Nenhum centroide pôde ser correspondido. Verifique se os códigos de município estão corretos."
        )


if __name__ == "__main__":
    gerar_centroides_fallback()

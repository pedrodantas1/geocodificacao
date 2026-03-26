import os

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point

# ============================
# CONFIGURAÇÕES
# ============================
PASTA_TRATADOS = "Dados_Tratados"
ARQUIVO_ORIGINAL = os.path.join(PASTA_TRATADOS, "Dim_Unidades_Saude.csv")
ARQUIVO_ERROS = "unidades_com_erro.csv"
ARQUIVO_MALHA_LOCAL = "malha_ibge_brasil.json"  # Novo: Cache local do mapa

URL_IBGE = "https://servicodados.ibge.gov.br/api/v3/malhas/paises/BR?formato=application/vnd.geo+json&qualidade=minima&intrarregiao=municipio"


def obter_malha_ibge():
    """Baixa a malha do IBGE de forma segura e salva em cache local."""
    if not os.path.exists(ARQUIVO_MALHA_LOCAL):
        print("   Baixando malha municipal do IBGE (apenas na primeira vez)...")
        # Disfarça a requisição como se fosse um navegador real para evitar bloqueios
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        response = requests.get(URL_IBGE, headers=headers)
        response.raise_for_status()  # Lança erro se o site do IBGE estiver fora do ar

        # Salva o resultado em um arquivo local
        with open(ARQUIVO_MALHA_LOCAL, "w", encoding="utf-8") as f:
            f.write(response.text)
        print("   Download concluído e salvo no cache local!")
    else:
        print("   Usando malha do IBGE em cache local (rápido)...")

    # Lê o arquivo local ao invés da URL
    return gpd.read_file(ARQUIVO_MALHA_LOCAL)


def auditar_espacialmente_com_ibge():
    print("--- 🗺️ INICIANDO AUDITORIA ESPACIAL (IBGE + GEOPANDAS) ---")

    if not os.path.exists(ARQUIVO_ORIGINAL):
        print(f"❌ Arquivo {ARQUIVO_ORIGINAL} não encontrado.")
        return

    # 1. Carregar os dados originais
    print("   Lendo arquivo CSV original...")
    df = pd.read_csv(ARQUIVO_ORIGINAL, sep=";", dtype=str)

    df["Lat_Num"] = pd.to_numeric(df["Latitude"].str.replace(",", "."), errors="coerce")
    df["Lng_Num"] = pd.to_numeric(
        df["Longitude"].str.replace(",", "."), errors="coerce"
    )

    df_sem_coord = df[
        df["Lat_Num"].isna() | df["Lng_Num"].isna() | (df["Lat_Num"] == 0)
    ].copy()
    df_sem_coord["Motivo_Erro"] = "Coordenada Vazia ou Inválida"

    df_com_coord = df.dropna(subset=["Lat_Num", "Lng_Num"]).copy()
    df_com_coord = df_com_coord[
        (df_com_coord["Lat_Num"] != 0) & (df_com_coord["Lng_Num"] != 0)
    ]

    if df_com_coord.empty:
        print("❌ Nenhuma coordenada válida para analisar no mapa.")
        return

    # 2. Criar a geometria matemática dos seus pontos
    print("   Criando geometria dos pontos das unidades de saúde...")
    geometry = [
        Point(xy) for xy in zip(df_com_coord["Lng_Num"], df_com_coord["Lat_Num"])
    ]
    gdf_unidades = gpd.GeoDataFrame(df_com_coord, geometry=geometry, crs="EPSG:4326")

    # 3. Baixar/Carregar a Malha Municipal do IBGE (Usando a nova função segura)
    gdf_ibge = obter_malha_ibge()
    gdf_ibge = gdf_ibge.rename(columns={"codarea": "ID_IBGE_Poligono"})

    # 4. Fazer o Cruzamento Espacial
    print("   Cruzando as coordenadas com o mapa do Brasil...")
    gdf_join = gpd.sjoin(gdf_unidades, gdf_ibge, how="left", predicate="intersects")

    # 5. Avaliar se a cidade do ponto bate com a cidade do seu CSV
    print("   Avaliando a precisão municipal...")
    registros_com_erro = []

    for index, row in gdf_join.iterrows():
        id_mun_csv = str(row.get("ID_Municipio", ""))[:6]

        # Pega o valor original ANTES de converter para string
        id_mapa_original = row.get("ID_IBGE_Poligono")
        id_mun_mapa = str(id_mapa_original).strip()

        # Checa se é nulo real (pd.isna), se a string ficou vazia, ou se virou 'nan' / 'None'
        if (
            pd.isna(id_mapa_original)
            or not id_mun_mapa
            or id_mun_mapa.lower() in ["nan", "none", "<na>"]
        ):
            registros_com_erro.append(
                {"CNES": row["CNES"], "Motivo_Erro": "Fora do Brasil ou no mar"}
            )
            continue

        if id_mun_csv and not id_mun_mapa.startswith(id_mun_csv):
            registros_com_erro.append(
                {
                    "CNES": row["CNES"],
                    "Motivo_Erro": f"Caiu em cidade errada. Esperado: IBGE {id_mun_csv}",
                }
            )

    # 6. Juntar os erros espaciais com os erros de coordenadas vazias e salvar
    df_erros_espaciais = pd.DataFrame(registros_com_erro)
    df_erros_finais = pd.concat(
        [df_sem_coord[["CNES", "Motivo_Erro"]], df_erros_espaciais], ignore_index=True
    )

    if not df_erros_finais.empty:
        df_erros_finais.to_csv(ARQUIVO_ERROS, sep=";", index=False)
        print(
            f"\n✅ Auditoria concluída! Encontrados {len(df_erros_finais)} registros problemáticos."
        )
        print(f"   Arquivo de erros salvo para correção em: {ARQUIVO_ERROS}")
    else:
        print(
            "\n✅ Auditoria concluída! Todas as coordenadas estão perfeitas e dentro das cidades corretas."
        )


if __name__ == "__main__":
    auditar_espacialmente_com_ibge()

import os
import time

import googlemaps
import pandas as pd
from tqdm import tqdm

# ============================
# CONFIGURAÇÕES
# ============================
GOOGLE_API_KEY = "AIzaSyC5KO1WWahti8ZMX242S9vCIuQe96V3WIs"  # <--- Coloque sua chave

PASTA_TRATADOS = "Dados_Tratados"
ARQUIVO_ORIGINAL = os.path.join(PASTA_TRATADOS, "Dim_Unidades_Saude.csv")
ARQUIVO_SAIDA_DELTA = "novas_coordenadas_google.csv"  # Arquivo só com os novos achados
ARQUIVO_MUNICIPIOS = os.path.join(PASTA_TRATADOS, "Dim_Geografia.csv")

# Tamanho do lote para salvar no disco
TAMANHO_LOTE = 50


def executar_geocodificacao_segura():
    print("--- 🌍 GEOCODIFICAÇÃO SEGURA (GOOGLE MAPS) ---")

    if not GOOGLE_API_KEY or "SUA_CHAVE" in GOOGLE_API_KEY:
        print("❌ Configure sua API KEY no script.")
        return

    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

    # 1. Ler arquivo original (Somente Leitura)
    print("   Lendo arquivo original...")
    df_cnes = pd.read_csv(ARQUIVO_ORIGINAL, sep=";", dtype=str)

    # 2. Ler Dicionário de Cidades
    df_mun = pd.read_csv(ARQUIVO_MUNICIPIOS, sep=";", dtype=str)
    dict_cidades = dict(zip(df_mun["ID_Municipio"], df_mun["Municipio"]))
    dict_ufs = dict(zip(df_mun["ID_Municipio"], df_mun["UF"]))

    # 3. Carregar o que já fizemos no arquivo Delta (para não repetir)
    cnes_ja_processados = set()
    if os.path.exists(ARQUIVO_SAIDA_DELTA):
        df_delta = pd.read_csv(ARQUIVO_SAIDA_DELTA, sep=";", dtype=str)
        cnes_ja_processados = set(df_delta["CNES"].unique())
        print(
            f"   Já existem {len(cnes_ja_processados)} registros processados no arquivo de saída."
        )

    # 4. Filtrar Pendentes
    # Critério: Lat vazia no original E CNES não está no arquivo Delta
    mask_vazio = (
        (df_cnes["Latitude"].isna())
        | (df_cnes["Latitude"] == "")
        | (df_cnes["Latitude"] == "0")
    )
    df_pendentes = df_cnes[mask_vazio].copy()

    # Remove os que já estão no Delta
    df_pendentes = df_pendentes[~df_pendentes["CNES"].isin(cnes_ja_processados)]

    total = len(df_pendentes)
    print(f"   Total para processar agora: {total}")

    if total == 0:
        print("✅ Nada novo para processar.")
        return

    if total > 1000:
        print(f"⚠️  ATENÇÃO: Você tem {total} registros para processar.")
        print("   O Google Maps cobra por requisição. Verifique sua cota.")
        input("   Pressione ENTER para continuar ou CTRL+C para cancelar...")

    # Buffer para salvar em lotes
    novos_achados = []

    for index, row in tqdm(df_pendentes.iterrows(), total=total):

        # --- PREPARAÇÃO DO ENDEREÇO ---
        id_mun = str(row.get("ID_Municipio", ""))[:6]
        cidade = dict_cidades.get(id_mun, "")
        uf = dict_ufs.get(id_mun, "")

        if not cidade:
            continue

        nome = str(row.get("Nome_Unidade", "")).strip()
        rua = str(row.get("Rua", "")).replace("S/N", "").strip()
        numero = str(row.get("Numero", "")).replace("S/N", "").strip()
        bairro = str(row.get("Bairro", "")).strip()

        # Estratégia de Busca
        queries = []
        if nome and rua:
            queries.append(f"{nome}, {rua}, {numero}, {cidade} - {uf}, Brasil")
        if nome and bairro:
            queries.append(f"{nome}, {bairro}, {cidade} - {uf}, Brasil")
        if rua:
            queries.append(f"{rua}, {numero}, {bairro}, {cidade} - {uf}, Brasil")

        lat_found, long_found, end_found = None, None, None

        for q in queries:
            try:
                res = gmaps.geocode(q, region="br")
                if res:
                    loc = res[0]["geometry"]["location"]
                    lat_found = str(loc["lat"])
                    long_found = str(loc["lng"])
                    end_found = res[0]["formatted_address"]
                    break
            except:
                time.sleep(1)  # Backoff simples em caso de erro
                continue

        if lat_found:
            novos_achados.append(
                {
                    "CNES": row["CNES"],
                    "Latitude_Nova": lat_found,
                    "Longitude_Nova": long_found,
                    "Endereco_Google": end_found,
                }
            )

        # --- SALVAMENTO INCREMENTAL ---
        if len(novos_achados) >= TAMANHO_LOTE:
            df_temp = pd.DataFrame(novos_achados)

            # Se o arquivo não existe, cria com cabeçalho. Se existe, append sem cabeçalho.
            modo = "a" if os.path.exists(ARQUIVO_SAIDA_DELTA) else "w"
            header = not os.path.exists(ARQUIVO_SAIDA_DELTA)

            df_temp.to_csv(
                ARQUIVO_SAIDA_DELTA, sep=";", index=False, mode=modo, header=header
            )

            novos_achados = []  # Limpa buffer

    # Salva o resto do buffer no final
    if novos_achados:
        df_temp = pd.DataFrame(novos_achados)
        modo = "a" if os.path.exists(ARQUIVO_SAIDA_DELTA) else "w"
        header = not os.path.exists(ARQUIVO_SAIDA_DELTA)
        df_temp.to_csv(
            ARQUIVO_SAIDA_DELTA, sep=";", index=False, mode=modo, header=header
        )

    print(f"\n✅ Finalizado! Novos dados salvos em: {ARQUIVO_SAIDA_DELTA}")


if __name__ == "__main__":
    executar_geocodificacao_segura()

import os
import time

import googlemaps
import pandas as pd
from tqdm import tqdm

# ============================
# CONFIGURAÇÕES
# ============================
GOOGLE_API_KEY = "AIzaSyCoUgKRkHhKlpfgOFj4GBIMUIIBS94MRMA"  # <--- Coloque sua chave

PASTA_TRATADOS = "Dados_Tratados"
PASTA_TRABALHO = "Correcoes"
ARQUIVO_ORIGINAL = os.path.join(PASTA_TRATADOS, "Dim_Unidades_Saude.csv")
ARQUIVO_MUNICIPIOS = os.path.join(PASTA_TRATADOS, "Dim_Geografia.csv")
ARQUIVO_ERROS = "unidades_com_erro.csv"  # Lista gerada pela auditoria
ARQUIVO_SAIDA_DELTA = os.path.join(
    PASTA_TRABALHO, "novas_coordenadas_google_robusto.csv"
)

TAMANHO_LOTE = 50


def executar_geocodificacao_robusta():
    print("--- 🌍 GEOCODIFICAÇÃO ROBUSTA (CASCATA & FILTROS) ---")

    if not GOOGLE_API_KEY or "SUA_CHAVE" in GOOGLE_API_KEY:
        print("❌ Configure sua API KEY no script.")
        return

    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

    if not os.path.exists(ARQUIVO_ERROS):
        print("❌ Arquivo de erros não encontrado.")
        return

    print("   Lendo bases de dados...")
    df_cnes = pd.read_csv(ARQUIVO_ORIGINAL, sep=";", dtype=str)
    df_erros = pd.read_csv(ARQUIVO_ERROS, sep=";", dtype=str)

    # Pega os registros originais baseados nos CNES que deram erro
    df_pendentes = df_cnes[df_cnes["CNES"].isin(df_erros["CNES"])].copy()

    df_mun = pd.read_csv(ARQUIVO_MUNICIPIOS, sep=";", dtype=str)
    dict_cidades = dict(zip(df_mun["ID_Municipio"], df_mun["Municipio"]))
    dict_ufs = dict(zip(df_mun["ID_Municipio"], df_mun["UF"]))

    # Controle de registros já processados para poder pausar e continuar
    cnes_ja_processados = set()
    if os.path.exists(ARQUIVO_SAIDA_DELTA):
        df_delta = pd.read_csv(ARQUIVO_SAIDA_DELTA, sep=";", dtype=str)
        if not df_delta.empty:
            cnes_ja_processados = set(df_delta["CNES"].unique())
        df_pendentes = df_pendentes[~df_pendentes["CNES"].isin(cnes_ja_processados)]

    total = len(df_pendentes)
    print(f"   Total para reprocessar: {total}")

    if total == 0:
        print("✅ Nada novo para processar.")
        return

    novos_achados = []

    for index, row in tqdm(df_pendentes.iterrows(), total=total):
        id_mun = str(row.get("ID_Municipio", ""))[:6]
        cidade = dict_cidades.get(id_mun, "")
        uf = dict_ufs.get(id_mun, "")

        if not cidade:
            continue

        nome = str(row.get("Nome_Unidade", "")).strip()
        rua = str(row.get("Rua", "")).replace("S/N", "").strip()
        numero = str(row.get("Numero", "")).replace("S/N", "").strip()
        bairro = str(row.get("Bairro", "")).strip()

        # ==========================================
        # ESTRATÉGIA DE CASCATA (Fallback)
        # ==========================================
        queries = []

        # Tentativa 1: Completa (Nome + Endereço + Cidade)
        if nome and rua:
            queries.append(f"{nome}, {rua}, {numero}, {bairro}, {cidade} - {uf}")

        # Tentativa 2: Só o Endereço (Ignora o Nome, caso o estabelecimento tenha mudado de nome)
        if rua:
            queries.append(f"{rua}, {numero}, {bairro}, {cidade} - {uf}")

        # Tentativa 3: Só o Nome + Bairro + Cidade (Caso a rua esteja escrita de forma muito confusa)
        if nome and bairro:
            queries.append(f"{nome}, {bairro}, {cidade} - {uf}")

        # Tentativa 4: Só o Nome + Cidade (Para quando o endereço original for muito pobre)
        if nome:
            queries.append(f"{nome}, {cidade} - {uf}")

        lat_found, long_found, end_found, loc_type = None, None, None, None

        for q in queries:
            try:
                # O parâmetro 'components' tranca a busca para evitar Falsos Positivos em outros estados
                res = gmaps.geocode(
                    q, components={"country": "BR", "administrative_area": uf}
                )

                if res:
                    loc_type = res[0]["geometry"]["location_type"]

                    # ACEITAÇÃO EXPANDIDA:
                    # ROOFTOP: Prédio exato (ideal para litoral, ignora auditoria do IBGE se achar isso)
                    # RANGE_INTERPOLATED: Interpolação na rua correta (resolve o ponto 5)
                    # GEOMETRIC_CENTER: Centro da via ou quadra
                    if loc_type in [
                        "ROOFTOP",
                        "RANGE_INTERPOLATED",
                        "GEOMETRIC_CENTER",
                    ]:

                        # Trava extra: Verifica se a cidade retornada pelo Google contém o nome esperado
                        # Isso previne que ele pegue a rua correta, mas na cidade vizinha
                        endereco_api = res[0]["formatted_address"].upper()
                        if cidade.upper() in endereco_api:
                            loc = res[0]["geometry"]["location"]
                            lat_found = str(loc["lat"])
                            long_found = str(loc["lng"])
                            end_found = res[0]["formatted_address"]
                            break  # Achou com qualidade boa, para a cascata

            except Exception as e:
                time.sleep(1)  # Backoff de segurança
                continue

        # Se após todas as tentativas a latitude foi encontrada, salva o registro
        if lat_found:
            novos_achados.append(
                {
                    "CNES": row["CNES"],
                    "Latitude_Nova": lat_found,
                    "Longitude_Nova": long_found,
                    "Endereco_Google": end_found,
                    "Qualidade_Busca": loc_type,
                }
            )

        # Salva em lotes para não perder os dados em caso de queda de internet
        if len(novos_achados) >= TAMANHO_LOTE:
            df_temp = pd.DataFrame(novos_achados)
            modo = "a" if os.path.exists(ARQUIVO_SAIDA_DELTA) else "w"
            header = not os.path.exists(ARQUIVO_SAIDA_DELTA)
            df_temp.to_csv(
                ARQUIVO_SAIDA_DELTA, sep=";", index=False, mode=modo, header=header
            )
            novos_achados = []

    # Salva o restinho que ficou no buffer
    if novos_achados:
        df_temp = pd.DataFrame(novos_achados)
        modo = "a" if os.path.exists(ARQUIVO_SAIDA_DELTA) else "w"
        header = not os.path.exists(ARQUIVO_SAIDA_DELTA)
        df_temp.to_csv(
            ARQUIVO_SAIDA_DELTA, sep=";", index=False, mode=modo, header=header
        )

    print(
        f"\n✅ Reprocessamento robusto finalizado! Correções salvas em: {ARQUIVO_SAIDA_DELTA}"
    )


if __name__ == "__main__":
    executar_geocodificacao_robusta()

import os
import time

import googlemaps
import pandas as pd
from tqdm import tqdm

# ============================
# CONFIGURAÇÕES
# ============================

# !!! COLOQUE SUA CHAVE DO GOOGLE AQUI !!!
GOOGLE_API_KEY = "preencher"

PASTA_TRATADOS = "Dados_Tratados"
ARQUIVO_CNES_ENTRADA = os.path.join(PASTA_TRATADOS, "Dim_Unidades_Saude.csv")
ARQUIVO_MUNICIPIOS = os.path.join(PASTA_TRATADOS, "Dim_Geografia.csv")
ARQUIVO_SAIDA_DELTA = "novas_coordenadas_google.csv"
ARQUIVO_CACHE = (
    "cache_google_maps.csv"  # Cache separado para não misturar com Nominatim
)

# Salvar a cada X registros para garantir segurança
TAMANHO_LOTE_SALVAMENTO = 50

# ============================
# PREPARAÇÃO
# ============================

if not GOOGLE_API_KEY or GOOGLE_API_KEY == "SUA_CHAVE_AQUI_VC_PEGA_NO_GOOGLE_CLOUD":
    print("❌ ERRO: Você precisa editar o script e colocar sua GOOGLE_API_KEY.")
    exit()

try:
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)
except Exception as e:
    print(f"❌ Erro ao iniciar cliente Google: {e}")
    exit()


def carregar_cache():
    if os.path.exists(ARQUIVO_CACHE):
        return pd.read_csv(ARQUIVO_CACHE, sep=";", dtype=str)
    return pd.DataFrame(
        columns=[
            "CNES",
            "Lat_Google",
            "Long_Google",
            "Endereco_Formatado_Google",
            "Tipo_Busca",
        ]
    )


def geocodificar_google_try(query):
    """Tenta geocodificar uma string de busca."""
    try:
        # Region 'br' ajuda a priorizar resultados no Brasil
        resultado = gmaps.geocode(query, region="br", language="pt-BR")

        if resultado and len(resultado) > 0:
            loc = resultado[0]["geometry"]["location"]
            formatted_address = resultado[0].get("formatted_address", "")
            return loc["lat"], loc["lng"], formatted_address
        return None, None, None
    except Exception as e:
        print(f"\n[ERRO API] {e}")
        return None, None, None


def executar_geocodificacao_google():
    print("--- 🌍 INICIANDO GEOCODIFICAÇÃO VIA GOOGLE MAPS ---")

    # 1. Carregar Dados
    if not os.path.exists(ARQUIVO_CNES_ENTRADA):
        print("❌ Arquivo Dim_Unidades_Saude.csv não encontrado.")
        return

    df_cnes = pd.read_csv(ARQUIVO_CNES_ENTRADA, sep=";", dtype=str)

    # 2. Carregar Dicionário de Cidades
    df_mun = pd.read_csv(ARQUIVO_MUNICIPIOS, sep=";", dtype=str)
    dict_cidades = dict(zip(df_mun["ID_Municipio"], df_mun["Municipio"]))
    dict_ufs = dict(zip(df_mun["ID_Municipio"], df_mun["UF"]))

    # 3. Carregar e Aplicar Cache
    df_cache = carregar_cache()
    if not df_cache.empty:
        print(f"   Carregando {len(df_cache)} registros do cache Google...")
        # Remove duplicatas
        df_cache = df_cache.drop_duplicates(subset=["CNES"], keep="last")

        # Merge com o dataframe principal
        df_cnes = pd.merge(
            df_cnes,
            df_cache[["CNES", "Lat_Google", "Long_Google"]],
            on="CNES",
            how="left",
        )

        # Onde tiver dado do Google, atualiza a coluna oficial Latitude/Longitude
        mask_google = df_cnes["Lat_Google"].notna()
        df_cnes.loc[mask_google, "Latitude"] = df_cnes.loc[mask_google, "Lat_Google"]
        df_cnes.loc[mask_google, "Longitude"] = df_cnes.loc[mask_google, "Long_Google"]

        # Limpa colunas auxiliares do merge
        df_cnes = df_cnes.drop(columns=["Lat_Google", "Long_Google"])

    # 4. Filtrar Pendentes (Quem não tem Latitude)
    mask_pendente = (
        (df_cnes["Latitude"].isna())
        | (df_cnes["Latitude"] == "")
        | (df_cnes["Latitude"] == "None")
        | (df_cnes["Latitude"] == "0")
    )
    df_pendentes = df_cnes[mask_pendente].copy()

    total = len(df_pendentes)
    print(f"   Unidades pendentes: {total}")

    if total == 0:
        print("✅ Tudo resolvido!")
        return

    # ALERTA DE CUSTO
    if total > 1000:
        print(f"⚠️  ATENÇÃO: Você tem {total} registros para processar.")
        print("   O Google Maps cobra por requisição. Verifique sua cota.")
        input("   Pressione ENTER para continuar ou CTRL+C para cancelar...")

    novos_cache = []
    contador = 0

    print("   Iniciando processamento...")

    for index, row in tqdm(df_pendentes.iterrows(), total=total):

        # Prepara dados básicos
        id_mun = str(row.get("ID_Municipio", ""))[:6]
        cidade = dict_cidades.get(id_mun, "")
        uf = dict_ufs.get(id_mun, "")

        if not cidade:
            # Sem cidade, impossível achar
            continue

        nome = str(row.get("Nome_Unidade", "")).strip()
        rua = str(row.get("Rua", "")).replace("S/N", "").strip()
        numero = str(row.get("Numero", "")).replace("S/N", "").strip()
        bairro = str(row.get("Bairro", "")).strip()

        lat_found, long_found, end_found, tipo_busca = None, None, None, None

        # --- ESTRATÉGIA DE 3 TENTATIVAS (Baseada no seu código) ---

        tentativas = []

        # Tentativa 1: Nome + Endereço + Cidade (O mais preciso)
        if nome and rua:
            t1 = f"{nome}, {rua}, {numero}, {cidade} - {uf}, Brasil"
            tentativas.append((t1, "Nome + Endereco"))

        # Tentativa 2: Nome + Bairro + Cidade (Ótimo para Postos de Saúde conhecidos)
        if nome and bairro:
            t2 = f"{nome}, {bairro}, {cidade} - {uf}, Brasil"
            tentativas.append((t2, "Nome + Bairro"))

        # Tentativa 3: Apenas Endereço (Se o nome estiver errado no Google)
        if rua:
            t3 = f"{rua}, {numero}, {bairro}, {cidade} - {uf}, Brasil"
            tentativas.append((t3, "Apenas Endereco"))

        # Executa tentativas
        for query, tipo in tentativas:
            lat, lng, address = geocodificar_google_try(query)
            if lat:
                lat_found = lat
                long_found = lng
                end_found = address
                tipo_busca = tipo
                break  # Achou? Para de tentar.

        # --- SALVAMENTO ---
        if lat_found:
            # Atualiza DF em memória
            df_cnes.at[index, "Latitude"] = str(lat_found)
            df_cnes.at[index, "Longitude"] = str(long_found)

            # Adiciona ao buffer do cache
            novos_cache.append(
                {
                    "CNES": row["CNES"],
                    "Lat_Google": str(lat_found),
                    "Long_Google": str(long_found),
                    "Endereco_Formatado_Google": end_found,
                    "Tipo_Busca": tipo_busca,
                }
            )

            contador += 1

        # Salva em disco a cada X registros
        if len(novos_cache) >= TAMANHO_LOTE_SALVAMENTO:
            df_novos = pd.DataFrame(novos_cache)
            df_antigo = carregar_cache()
            df_full = pd.concat([df_antigo, df_novos], ignore_index=True)
            df_full = df_full.drop_duplicates(subset=["CNES"], keep="last")

            df_full.to_csv(ARQUIVO_CACHE, sep=";", index=False)
            df_cnes.to_csv(ARQUIVO_CNES_ENTRADA, sep=";", index=False)
            # df_cnes.to_csv('Dim_Unidades_Saude_TESTE.csv', sep=';', index=False)

            novos_cache = []  # Limpa buffer

    # Salvamento final
    if novos_cache:
        df_novos = pd.DataFrame(novos_cache)
        df_full = pd.concat([carregar_cache(), df_novos], ignore_index=True)
        df_full.drop_duplicates(subset=["CNES"], keep="last").to_csv(
            ARQUIVO_CACHE, sep=";", index=False
        )
        df_cnes.to_csv(ARQUIVO_CNES_ENTRADA, sep=";", index=False)
        # df_cnes.to_csv('Dim_Unidades_Saude_TESTE.csv', sep=';', index=False)

    print("\n✅ Processo Google Maps finalizado!")


if __name__ == "__main__":
    executar_geocodificacao_google()

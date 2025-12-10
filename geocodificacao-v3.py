import pandas as pd
import requests
import googlemaps
import os

# ============================
# CONFIGURAÇÕES
# ============================

INPUT_FILE = "aracaju_sample.csv"
OUTPUT_FILE = "coordenadas_aracaju_google_maps.csv"
CACHE_FILE = "cache_geocode.csv"

CNES_API = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos/"

GOOGLE_API_KEY = "preencher"
gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

CODIGO_UF = 28        # Sergipe
CODIGO_CIDADE = 2800308   # Aracaju

# ============================
# CARREGAR CACHE
# ============================

if os.path.exists(CACHE_FILE):
    cache_df = pd.read_csv(CACHE_FILE, dtype=str)
else:
    cache_df = pd.DataFrame(columns=["ID_UNIDADE", "lat", "lon", "endereco_usado"])

cache = {
    row["ID_UNIDADE"]: {
        "nome": row["nome"],
        "lat": row["lat"],
        "lon": row["lon"],
        "endereco_usado": row["endereco_usado"]
    }
    for _, row in cache_df.iterrows()
}

print(f"Cache carregado com {len(cache)} entradas.")


# ============================
# FUNÇÕES AUXILIARES
# ============================

def valor_preenchido(v):
    return (
        v not in [None, "", " ", "None"] and
        str(v).strip().lower() not in ["nan", "none", "null"]
    )


def consulta_cnes(cnes):
    """Consulta a API do CNES e retorna dados do estabelecimento."""
    try:
        resp = requests.get(CNES_API + str(int(cnes)))
        if resp.status_code != 200:
            return None

        est = resp.json()

        return {
            "nome": est.get("nome_fantasia", ""),
            "logradouro": est.get("endereco_estabelecimento", ""),
            "numero": est.get("numero_estabelecimento", ""),
            "bairro": est.get("bairro_estabelecimento", ""),
            "municipio": est.get("nome_municipio", est.get("codigo_municipio", "")),
            "uf": est.get("codigo_uf", "")
        }
    except:
        return None


def limpar_valor(x):
    if x and str(x).strip() not in ["", "nan", "None", "S/N", "S-N"]:
        return str(x).strip()
    return None


def montar_endereco(info):
    partes = []

    for campo in ["logradouro", "numero", "bairro", "municipio", "uf"]:
        v = limpar_valor(info.get(campo))
        if v:
            partes.append(v)

    return ", ".join(partes)


def geocodificar_google(endereco):
    """Geocodifica usando a API do Google Maps."""
    try:
        if not endereco:
            return None, None

        resultado = gmaps.geocode(endereco)

        if resultado and len(resultado) > 0:
            loc = resultado[0]["geometry"]["location"]
            return loc["lat"], loc["lng"]

        return None, None

    except Exception as e:
        print(f"[ERRO GOOGLE] {e} | Endereço: {endereco}")
        return None, None


def geocodificar_melhorado(info):
    """3 tentativas de geocodificação — primeira por NOME."""

    tentativas = []

    # 1 — Nome da unidade + endereço + Aracaju
    if info.get("nome"):
        end1 = f"{info['nome']}, {montar_endereco(info)}"
        tentativas.append(end1)

    # 2 — Endereço completo convencional
    end2 = montar_endereco(info)
    tentativas.append(end2)

    # 3 — Apenas nome + bairro (funciona muito bem em unidades de saúde)
    if info.get("nome") and info.get("bairro"):
        end3 = f"{info['nome']}, {info['bairro']}"
        tentativas.append(end3)

    # Execução das tentativas
    for e in tentativas:
        lat, lon = geocodificar_google(e)
        if lat is not None and lon is not None:
            return lat, lon, e

    return None, None, None


# ============================
# PROCESSAMENTO PRINCIPAL
# ============================

df = pd.read_csv(INPUT_FILE, dtype=str)
cnes_unicos = df["ID_UNIDADE"].dropna().unique()

print(f"Processando {len(cnes_unicos)} CNES únicos...\n")

resultados = []

for cnes in cnes_unicos:

    # 1 — Verifica cache apenas se COMPLETO
    if cnes in cache:
        nome_cache = cache[cnes]["nome"]
        lat_cache = cache[cnes]["lat"]
        lon_cache = cache[cnes]["lon"]
        end_cache = cache[cnes]["endereco_usado"]

        if valor_preenchido(nome_cache) and valor_preenchido(lat_cache) and valor_preenchido(lon_cache) and valor_preenchido(end_cache):
            print(f"[CACHE] CNES {cnes}: {end_cache}")
            resultados.append({
                "ID_UNIDADE": cnes,
                "nome": nome_cache,
                "lat": lat_cache,
                "lon": lon_cache,
                "endereco_usado": end_cache
            })
            continue
        else:
            print(f"[CACHE INCOMPLETO] CNES {cnes}: recalculando...")

    # 2 — Consulta CNES
    info = consulta_cnes(cnes)

    if info is None:
        print(f"[ERRO CNES] Não encontrado para CNES {cnes}")
        resultados.append({
            "ID_UNIDADE": cnes, "nome": info['nome'], "lat": None, "lon": None, "endereco_usado": None
        })
        continue

    # Correções do IBGE
    if info["uf"] == CODIGO_UF:
        info["uf"] = "SE"
    if str(info["municipio"]).startswith(str(CODIGO_CIDADE)[:6]):
        info["municipio"] = "Aracaju"

    # 3 — Geocodificação
    lat, lon, usado = geocodificar_melhorado(info)

    if lat is None:
        print(f"[FALHA] CNES {cnes}: nenhuma tentativa funcionou")

    resultados.append({
        "ID_UNIDADE": cnes,
        "nome": info['nome'],
        "lat": lat,
        "lon": lon,
        "endereco_usado": usado
    })

    # Atualiza cache
    cache[cnes] = {
        "nome": info['nome'],
        "lat": lat,
        "lon": lon,
        "endereco_usado": usado
    }


# ============================
# SALVAR RESULTADOS E CACHE
# ============================

df_final = pd.DataFrame(resultados)
df_final.to_csv(OUTPUT_FILE, index=False)

cache_df = pd.DataFrame([
    {
        "ID_UNIDADE": k,
        "nome": v.get("nome"),
        "lat": v.get("lat"),
        "lon": v.get("lon"),
        "endereco_usado": v.get("endereco_usado")
    }
    for k, v in cache.items()
])
cache_df.to_csv(CACHE_FILE, index=False)

print("\nProcesso concluído.")
print(f"Arquivo gerado: {OUTPUT_FILE}")
print(f"Cache atualizado: {CACHE_FILE}")

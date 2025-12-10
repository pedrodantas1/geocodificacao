import pandas as pd
import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm
import os

# ============================
# CONFIGURAÇÕES
# ============================

INPUT_FILE = "aracaju_sample.csv"
OUTPUT_FILE = "coordenadas_aracaju.csv"
CACHE_FILE = "cache_geocode.csv"

CNES_API = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos/"

# Codigo da UF Sergipe
CODIGO_UF = 28

# Codigo da cidade de Aracaju
CODIGO_CIDADE = 2800308

# Configura geocodificador
geolocator = Nominatim(user_agent="geocoding_sus/1.0 (monitorasus@exemplo.com)", timeout=10)

geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=1.0,
    max_retries=2,
    error_wait_seconds=2.0
)

# ============================
# CARREGAR CACHE LOCAL
# ============================

if os.path.exists(CACHE_FILE):
    cache_df = pd.read_csv(CACHE_FILE, dtype=str)
else:
    cache_df = pd.DataFrame(columns=["ID_UNIDADE","lat","lon","endereco_usado"])

cache = {
    row["ID_UNIDADE"]: {
        "lat": row["lat"],
        "lon": row["lon"],
        "endereco_usado": row["endereco_usado"]
    }
    for _, row in cache_df.iterrows()
}

print(f"Cache carregado com {len(cache)} entradas.")


# ============================
# FUNÇÕES
# ============================

def consulta_cnes(cnes):
    """Consulta a API do CNES e retorna dados relevantes."""
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


def geocodificar(endereco):
    try:
        if not endereco:
            return None, None
        loc = geocode(str(endereco))
        if loc:
            return float(loc.latitude), float(loc.longitude)
    except:
        pass
    return None, None


def geocodificar_melhorado(info):
    """Várias tentativas de geocodificação."""

    tentativas = []

    end1 = montar_endereco(info)
    tentativas.append(end1)

    end2 = ", ".join([limpar_valor(info.get("logradouro")) or "",
                      limpar_valor(info.get("bairro")) or "",
                      info.get("municipio"), "SE"])
    tentativas.append(end2)

    end3 = ", ".join([limpar_valor(info.get("logradouro")) or "",
                      info.get("municipio"), "SE"])
    tentativas.append(end3)

    if info.get("nome"):
        end4 = f"{info['nome']}, {info['municipio']}, SE"
        tentativas.append(end4)

    if info.get("nome") and info.get("bairro"):
        end5 = f"{info['nome']}, {info['bairro']}, {info['municipio']}, SE"
        tentativas.append(end5)

    # Executa as tentativas
    for e in tentativas:
        lat, lon = geocodificar(e)
        if lat is not None and lon is not None:
            return lat, lon, e

    return None, None, None


# ============================
# PROCESSAR CNES
# ============================

df = pd.read_csv(INPUT_FILE, dtype=str)
cnes_unicos = df["ID_UNIDADE"].dropna().unique()

print(f"Processando {len(cnes_unicos)} CNES únicos...")

resultados = []

for cnes in tqdm(cnes_unicos):

    # 1 — Verificar cache
    if cnes in cache:
        resultados.append({
            "ID_UNIDADE": cnes,
            "lat": cache[cnes]["lat"],
            "lon": cache[cnes]["lon"],
            "endereco_usado": cache[cnes]["endereco_usado"]
        })
        continue

    # 2 — Consultar CNES
    info = consulta_cnes(cnes)
    if info is None:
        continue

    # Ajuste especifico de UF e Municipio
    if info['uf'] == CODIGO_UF:
        info['uf'] = "SE"
    if str(info['municipio']) == str(CODIGO_CIDADE)[:6]:
        info['municipio'] = "Aracaju"

    # 3 — Geocodificar com fallback
    lat, lon, usado = geocodificar_melhorado(info)

    resultados.append({
        "ID_UNIDADE": cnes,
        "lat": lat,
        "lon": lon,
        "endereco_usado": usado
    })

    # 4 — Atualizar cache em memória
    cache[cnes] = {
        "lat": lat,
        "lon": lon,
        "endereco_usado": usado
    }


# ============================
# SALVAR RESULTADOS E CACHE
# ============================

# Resultado final
df_final = pd.DataFrame(resultados)
df_final.to_csv(OUTPUT_FILE, index=False)

# Atualizar cache
cache_df = pd.DataFrame([
    {"ID_UNIDADE": k, "lat": v["lat"], "lon": v["lon"], "endereco_usado": v["endereco_usado"]}
    for k, v in cache.items()
])
cache_df.to_csv(CACHE_FILE, index=False)

print("\nProcesso concluído.")
print(f"Arquivo gerado: {OUTPUT_FILE}")
print(f"Cache atualizado: {CACHE_FILE}")

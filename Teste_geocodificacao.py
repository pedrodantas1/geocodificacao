import pandas as pd
import requests
import time
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

# ============================
# CONFIGURAÇÕES
# ============================

# Caminho para seu sample de Aracaju
INPUT_FILE = "aracaju_sample.csv"

# Arquivo final com lat/long
OUTPUT_FILE = "coordernadas_aracaju.csv"

# Codigo da UF Sergipe
CODIGO_UF = 28

# Codigo da cidade de Aracaju
CODIGO_CIDADE = 2800308

# API do CNES
CNES_API = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos/"

# Configura geocodificador
geolocator = Nominatim(user_agent="geocoding_sus/1.0 (monitorasus@exemplo.com)", timeout=10)

# 2) RateLimiter garante no mínimo 1 segundo entre chamadas (min_delay_seconds=1)
geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=1.0,   # respeita o limite público do Nominatim
    max_retries=2,
    error_wait_seconds=2.0
)


# ============================
# FUNÇÕES AUXILIARES
# ============================

def consulta_cnes(cnes):
    """Consulta a API do CNES e retorna o endereço."""
    try:
        uri_req = CNES_API + str(int(cnes))
        resp = requests.get(uri_req)
        if resp.status_code != 200:
            return None
        data = resp.json()

        if len(data) == 0:
            return None

        est = data

        return {
            "nome": est.get("nome_fantasia", ""),
            "logradouro": est.get("endereco_estabelecimento", ""),
            "numero": est.get("numero_estabelecimento", ""),
            "bairro": est.get("bairro_estabelecimento", ""),
            "cep": est.get("codigo_cep_estabelecimento", ""),
            "municipio": est.get("codigo_municipio", ""),
            "uf": est.get("codigo_uf", "")
        }
    except:
        return None


def geocodificar(endereco):
    """Transforma endereço em lat/long usando OpenStreetMap."""
    try:
        if not endereco or str(endereco).strip() == "":
            return None, None
        location = geocode(str(endereco))
        if location:
            return float(location.latitude), float(location.longitude)
        return None, None
    except Exception:
        logging.exception("Erro ao geocodificar: %s", endereco)
        return None, None

def montar_endereco(info):
    partes = []

    def add(x):
        if x and str(x).strip() not in ["", "None", "nan", "S/N", "S-N"]:
            partes.append(str(x).strip())

    add(info.get("logradouro"))
    add(info.get("numero"))
    add(info.get("bairro"))
    add(info.get("municipio"))
    add(info.get("uf"))

    # junta com vírgula
    return ", ".join(partes)


# ============================
# ETAPA 1 — Carregar o sample
# ============================

df = pd.read_csv(INPUT_FILE, dtype=str)

# Pega CNES únicos (ID_UNIDADE)
cnes_unicos = df["ID_UNIDADE"].dropna().unique()

print(f"Encontrados {len(cnes_unicos)} CNES em Aracaju.")


# ============================
# ETAPA 2 — Baixar dados + Geocodificar
# ============================

resultados = []

for cnes in cnes_unicos:
    info = consulta_cnes(cnes)
    
    if info is None:
        resultados.append({
            "ID_UNIDADE": cnes,
            "nome": None,
            "endereco": None,
            "latitude": None,
            "longitude": None
        })
        continue
    
    # monta endereço para geocodificação
    # Filtro especifico
    if info['uf'] == CODIGO_UF:
        info['uf'] = "SE"
    if str(info['municipio']) == str(CODIGO_CIDADE)[:6]:
        info['municipio'] = "Aracaju"
        
    endereco_formatado = montar_endereco(info)
    print("ENVIANDO PARA OSM:", endereco_formatado)
    lat, lon = geocodificar(endereco_formatado)
    print(lat)
    print(lon)

    resultados.append({
        "ID_UNIDADE": cnes,
        "nome": info['nome'],
        "endereco": endereco_formatado,
        "latitude": lat,
        "longitude": lon
    })

# ============================
# ETAPA 3 — Gerar CSV final
# ============================

tabela_final = pd.DataFrame(resultados)
tabela_final.to_csv(OUTPUT_FILE, index=False)

print("\nArquivo final gerado:")
print(OUTPUT_FILE)
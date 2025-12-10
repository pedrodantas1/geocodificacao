import googlemaps

# Sua chave da API do Google
GOOGLE_API_KEY = "AIzaSyDkoqPQcARtILXEr90dvDuAEaHr0F91_8A"

# Inicializa o cliente
gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

def geocode_unidade(nome_unidade, endereco=None, cidade="Aracaju", uf="SE"):
    """
    Busca coordenadas de uma unidade de saúde no Google Maps.
    Pode usar:
      - nome + cidade
      - nome + endereco + cidade
    """
    if endereco:
        query = f"{nome_unidade}, {endereco}, {cidade}, {uf}"
    else:
        query = f"{nome_unidade}, {cidade}, {uf}"

    print(f"Buscando: {query}")

    try:
        resultado = gmaps.geocode(query)

        if resultado:
            loc = resultado[0]["geometry"]["location"]
            return loc["lat"], loc["lng"]

        return None, None

    except Exception as e:
        print("Erro ao consultar Google:", e)
        return None, None


# ================================
# Dados de entrada
# ================================

nome = "US SANTA TEREZINHA ROBALO"
endereco = "RODOVIA DOS NAUFRAGOS KM 05"

lat, lon = geocode_unidade(nome, endereco)

print(f"Latitude, longitude: {lat}, {lon}")

import pandas as pd
import os
import glob
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# CONFIGURAÇÃO DE PASTAS
PASTA_BRUTOS = 'Dados_Brutos'
PASTA_AUXILIARES = 'Dados_Auxiliares'
PASTA_TRATADOS = 'Dados_Tratados'

# Garante que a pasta de saída existe
os.makedirs(PASTA_TRATADOS, exist_ok=True)

def tratar_codigo_ibge(valor):
    """
    Padroniza o código IBGE para 6 dígitos (Padrão SUS/SINAN).
    Remove o último dígito verificador se tiver 7 dígitos.
    """
    try:
        s_codigo = str(int(float(valor)))  # Remove decimais e converte para string
        if len(s_codigo) == 7:
            return s_codigo[:6]  # Corta o último dígito
        return s_codigo.zfill(6) # Garante que tenha zeros à esquerda se necessário
    except:
        return None

def carregar_dimensao_geografia():
    print("--- 1. Criando Dimensão Geografia Unificada ---")
    try:
        # Carregar tabelas (ajuste o separador se seu CSV usar vírgula)
        df_mun = pd.read_csv(os.path.join(PASTA_AUXILIARES, 'municipios.csv'), sep=',', encoding='utf-8') # ou latin1
        df_est = pd.read_csv(os.path.join(PASTA_AUXILIARES, 'estados.csv'), sep=',', encoding='utf-8')

        # Unificar Municípios com Estados
        # O 'codigo_uf' é a chave comum (ex: 35 para SP)
        df_geo = pd.merge(df_mun, df_est, on='codigo_uf', how='left', suffixes=('', '_est'))

        # Criar a chave de ligação (ID_Municipio) padronizada
        df_geo['ID_Municipio'] = df_geo['codigo_ibge'].apply(tratar_codigo_ibge)

        # Selecionar apenas colunas úteis para o BI (Latitude/Longitude aqui são vitais)
        colunas_finais = {
            'ID_Municipio': 'ID_Municipio',
            'nome': 'Municipio',
            'latitude': 'Latitude',
            'longitude': 'Longitude',
            'uf': 'UF',
            'nome_est': 'Estado',
            'regiao': 'Regiao'
        }
        
        # Renomear e filtrar
        df_geo_final = df_geo.rename(columns=colunas_finais)
        df_geo_final = df_geo_final[list(colunas_finais.values())]
        
        # Salvar
        caminho_saida = os.path.join(PASTA_TRATADOS, 'Dim_Geografia.csv')
        df_geo_final.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8')
        print(f"✅ Dimensão Geografia salva com sucesso! ({len(df_geo_final)} registros)")
        return True
    except Exception as e:
        print(f"❌ Erro na Dimensão Geografia: {e}")
        return False

def processar_fatos_dengue():
    print("\n--- 2. Processando Fatos (Casos de Dengue) ---")
    
    arquivos = glob.glob(os.path.join(PASTA_BRUTOS, '*.csv'))
    
    # LISTA DE COLUNAS IMPORTANTES (Use os nomes exatos do seu CSV)
    # Geralmente no SINAN são estes nomes abreviados. 
    # Se der erro de "KeyError", verifique o cabeçalho do seu arquivo.
    colunas_para_ler = [
        'DT_NOTIFIC',  # Data da notificação
        'ID_MUNICIP',  # Código IBGE do Município (as vezes vem como ID_MUNICIP)
        'ID_UNIDADE',  # Código CNES
        'NU_ANO',      # Ano
        # Adicione aqui outras se precisar, ex: 'DT_SIN_PRI' (Data Sintomas)
    ]

    lista_dfs = []

    for arquivo in arquivos:
        print(f"   Lendo: {os.path.basename(arquivo)}...")
        try:
            # usecols: Lê apenas as colunas especificadas -> Resolve o PerformanceWarning
            # dtype=str: Lê tudo como texto inicialmente -> Resolve o DtypeWarning
            df_temp = pd.read_csv(
                arquivo, 
                sep=';',  # Tente ; primeiro
                encoding='latin1', 
                usecols=lambda c: c in colunas_para_ler, # Filtra só as colunas que existem na lista
                dtype=str # Força tudo como texto para evitar erro de tipo misto
            )
            
            # Se o arquivo usar vírgula em vez de ponto e vírgula, tenta de novo
            if df_temp.empty or df_temp.shape[1] < 2:
                df_temp = pd.read_csv(
                    arquivo, 
                    sep=',', 
                    encoding='latin1',
                    usecols=lambda c: c in colunas_para_ler,
                    dtype=str
                )

            lista_dfs.append(df_temp)
            
        except Exception as e:
            print(f"   ⚠️ Erro ao ler {arquivo}: {e}")

    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        
        # Renomear colunas para facilitar
        mapa_colunas = {
            'DT_NOTIFIC': 'Data_Notificacao',
            'ID_MUNICIP': 'ID_Municipio',
            'ID_UNIDADE': 'CNES',
            'NU_ANO': 'Ano'
        }
        df_consolidado = df_consolidado.rename(columns=mapa_colunas)

        print("   Tratando datas e códigos...")
        
        # Tratamento de Data
        df_consolidado['Data_Notificacao'] = pd.to_datetime(
            df_consolidado['Data_Notificacao'], 
            errors='coerce'
        )

        # Tratamento do Código IBGE (Função que já criamos)
        if 'ID_Municipio' in df_consolidado.columns:
            df_consolidado['ID_Municipio'] = df_consolidado['ID_Municipio'].apply(tratar_codigo_ibge)

        # Salvar
        caminho_saida = os.path.join(PASTA_TRATADOS, 'Fato_Dengue_Consolidada.csv')
        df_consolidado.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8')
        print(f"✅ Base Dengue salva! ({len(df_consolidado)} registros)")
    else:
        print("❌ Nenhum dado processado.")
    
def criar_dimensao_unidades_saude(usar_api_para_preencher_vazios=False):
    print("\n--- 3. Criando Dimensão Unidades de Saúde (CNES) ---")
    
    arquivo_cnes = os.path.join(PASTA_AUXILIARES, 'tbEstabelecimento.csv')
    
    if not os.path.exists(arquivo_cnes):
        print("❌ Arquivo 'tbEstabelecimento.csv' não encontrado em Dados_Auxiliares.")
        print("   Baixe do CNES/DataSUS. Campos esperados: CNES, NOME, LATITUDE, LONGITUDE, LOGRADOURO...")
        return

    # Mapeamento de colunas (ajuste conforme o CSV que você baixar)
    colunas_para_ler = {
        'CO_CNES': 'CNES',
        'NO_FANTASIA': 'Nome_Unidade',
        'NU_LATITUDE': 'Latitude',
        'NU_LONGITUDE': 'Longitude',
        'NO_LOGRADOURO': 'Rua',
        'NU_ENDERECO': 'Numero',
        'NO_BAIRRO': 'Bairro'
    }
        
    try:
        # Carregando base oficial (simulando colunas comuns do DataSUS)
        df_cnes = pd.read_csv(arquivo_cnes, sep=';',
                            encoding='latin1', 
                            usecols=lambda c: c in colunas_para_ler,
                            dtype=str)
        
        df_unidades = df_cnes.rename(columns=colunas_para_ler)
        
        # Selecionar apenas o necessário
        cols_finais = list(colunas_para_ler.values())
        # Garante que as colunas existem antes de filtrar
        cols_finais = [c for c in cols_finais if c in df_unidades.columns]
        df_unidades = df_unidades[cols_finais]

        # Tratamento básico de coordenadas (converter vírgula para ponto)
        # Muitos dados do governo vêm como "-23,55"
        for col in ['Latitude', 'Longitude']:
            if col in df_unidades.columns:
                df_unidades[col] = df_unidades[col].astype(str).str.replace(',', '.').replace('nan', None)

        # --- GEOCODING (O PULO DO GATO) ---
        if usar_api_para_preencher_vazios:
            print("   Iniciando preenchimento de coordenadas via API (Isso pode demorar)...")
            
            geolocator = Nominatim(user_agent="meu_bi_dengue_app")
            geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

            # Filtra apenas quem não tem latitude definida
            mask_sem_geo = (df_unidades['Latitude'].isnull()) | (df_unidades['Latitude'] == '') | (df_unidades['Latitude'] == 'None')
            
            # Função auxiliar para criar endereço completo
            def criar_endereco(row):
                # É ideal ter o nome da cidade aqui. Se não tiver no CSV do CNES,
                # precisaria cruzar com a tabela de municipios antes.
                # Assumindo formato simples:
                return f"{row.get('Rua','')}, {row.get('Numero','')}, {row.get('Bairro','')}, Brasil"

            # Aplica apenas nas linhas vazias (limitado a 10 para teste, remova o .head(10) em produção)
            print(f"   Tentando geolocalizar {df_unidades[mask_sem_geo].shape[0]} unidades...")
            
            # CUIDADO: Fazer isso para 50 mil linhas vai demorar dias e bloquear seu IP no Nominatim.
            # Use APIs pagas (Google/Bing) se o volume for alto.
            
            for index, row in df_unidades[mask_sem_geo].head(10).iterrows():
                endereco = criar_endereco(row)
                try:
                    location = geocode(endereco)
                    if location:
                        df_unidades.at[index, 'Latitude'] = location.latitude
                        df_unidades.at[index, 'Longitude'] = location.longitude
                        print(f"   📍 Encontrado: {row['Nome_Unidade']}")
                except:
                    pass

        # Salvar Dimensão CNES
        df_unidades.to_csv(os.path.join(PASTA_TRATADOS, 'Dim_Unidades_Saude.csv'), index=False, sep=';')
        print(f"✅ Dimensão Unidades de Saúde salva ({len(df_unidades)} registros).")

    except Exception as e:
        print(f"❌ Erro ao processar CNES: {e}")

if __name__ == "__main__":
    # carregar_dimensao_geografia()
    # processar_fatos_dengue()
    criar_dimensao_unidades_saude(usar_api_para_preencher_vazios=False)
    print("\nProcesso finalizado! Pode atualizar o BI.")
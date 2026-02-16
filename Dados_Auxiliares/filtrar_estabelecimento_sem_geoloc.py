import pandas as pd

# CONFIGURAÇÕES
ARQUIVO_ENTRADA = 'estabelecimentos_sergipe.csv'   # arquivo filtrado de Sergipe
ARQUIVO_SAIDA = 'sergipe_sem_geolocalizacao.csv'  # arquivo de saída

def filtrar_sem_geolocalizacao():
    print(f"Lendo o arquivo: {ARQUIVO_ENTRADA}...")

    try:
        # Lê o CSV
        df = pd.read_csv(
            ARQUIVO_ENTRADA,
            sep=';',
            encoding='utf-8',  # já gerado pelo seu script anterior
            dtype={'NU_LATITUDE': str, 'NU_LONGITUDE': str},
            low_memory=False
        )

        # Verifica se as colunas existem
        for col in ['NU_LATITUDE', 'NU_LONGITUDE']:
            if col not in df.columns:
                print(f"❌ Erro: Coluna '{col}' não encontrada no CSV.")
                return

        # Filtra linhas onde latitude ou longitude estão vazias ou nulas
        df_sem_geo = df[
            df['NU_LATITUDE'].isna() | df['NU_LATITUDE'].str.strip().eq('') |
            df['NU_LONGITUDE'].isna() | df['NU_LONGITUDE'].str.strip().eq('')
        ]

        qtd_registros = len(df_sem_geo)

        if qtd_registros > 0:
            df_sem_geo.to_csv(ARQUIVO_SAIDA, index=False, sep=';', encoding='utf-8')
            print(f"✅ Sucesso! {qtd_registros} estabelecimentos sem geolocalização salvos em '{ARQUIVO_SAIDA}'")
        else:
            print("⚠️ Nenhum registro com latitude ou longitude vazia encontrado.")

    except FileNotFoundError:
        print(f"❌ O arquivo '{ARQUIVO_ENTRADA}' não foi encontrado.")
    except Exception as e:
        print(f"❌ Ocorreu um erro: {e}")

if __name__ == "__main__":
    filtrar_sem_geolocalizacao()

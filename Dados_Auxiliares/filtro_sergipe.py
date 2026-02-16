import pandas as pd
import os

# CONFIGURAÇÕES
# Substitua pelo nome exato do seu arquivo original se for diferente
ARQUIVO_ENTRADA = 'tbEstabelecimento.csv' 
ARQUIVO_SAIDA = 'estabelecimentos_sergipe.csv'
CODIGO_UF_SERGIPE = 28

def filtrar_dados_sergipe():
    print(f"Lendo o arquivo: {ARQUIVO_ENTRADA}...")
    
    try:
        # Lê o CSV. 
        # sep=';' é o padrão do DataSUS.
        # encoding='latin1' (ou 'cp1252') é o padrão de arquivos do governo brasileiro.
        # dtype={'CO_UF': str} garante que o código do estado seja lido como texto para não perder zeros ou dar erro.
        df = pd.read_csv(
            ARQUIVO_ENTRADA, 
            sep=';', 
            encoding='latin1', 
            dtype={'CO_UF': str, 'CO_CNES': str, 'CO_IBGE': str},
            low_memory=False
        )
        
        # Verifica se a coluna CO_UF existe (as vezes vem com nomes diferentes ou espaços)
        if 'CO_UF' not in df.columns:
            print("❌ Erro: Coluna 'CO_UF' não encontrada. Verifique o cabeçalho do CSV.")
            print(f"Colunas encontradas: {df.columns.tolist()}")
            return

        # FILTRAGEM
        print("Filtrando estabelecimentos de Sergipe (Código 28)...")
        # Filtra onde CO_UF é igual a '28'
        df_sergipe = df[df['CO_UF'] == str(CODIGO_UF_SERGIPE)]
        
        qtd_registros = len(df_sergipe)
        
        if qtd_registros > 0:
            # Salva o novo arquivo
            df_sergipe.to_csv(ARQUIVO_SAIDA, index=False, sep=';', encoding='utf-8')
            print(f"✅ Sucesso! Arquivo '{ARQUIVO_SAIDA}' gerado com {qtd_registros} estabelecimentos.")
            
            # Mostra uma prévia das colunas de Localização para você conferir
            print("\n--- Prévia dos dados (Colunas de Localização) ---")
            cols_verificacao = ['CO_CNES', 'NO_FANTASIA', 'NU_LATITUDE', 'NU_LONGITUDE']
            # Filtra apenas colunas que realmente existem no df para não dar erro no print
            cols_existentes = [c for c in cols_verificacao if c in df_sergipe.columns]
            print(df_sergipe[cols_existentes].head().to_markdown(index=False))
            
        else:
            print("⚠️ Nenhum registro encontrado com CO_UF = 28. Verifique se o código do estado no arquivo está correto.")

    except FileNotFoundError:
        print(f"❌ O arquivo '{ARQUIVO_ENTRADA}' não foi encontrado na pasta.")
    except Exception as e:
        print(f"❌ Ocorreu um erro: {e}")

if __name__ == "__main__":
    filtrar_dados_sergipe()
import os

import pandas as pd

# ============================
# CONFIGURAÇÕES
# ============================
PASTA_TRATADOS = "Dados_Tratados"
PASTA_TRABALHO = "Correcoes"
ARQUIVO_PRINCIPAL = os.path.join(PASTA_TRATADOS, "Dim_Unidades_Saude.csv")
ARQUIVO_ERROS = "unidades_com_erro.csv"  # Lista com os ~25k
ARQUIVO_DELTA = os.path.join(
    PASTA_TRABALHO, "novas_coordenadas_google_robusto.csv"
)  # Lista com os ~23k
ARQUIVO_FALHAS = os.path.join(PASTA_TRABALHO, "cnes_nao_encontrados.csv")  # Saída


def analisar_falhas():
    print("--- 🔍 ANALISANDO FALHAS DE GEOCODIFICAÇÃO ---")

    if not os.path.exists(ARQUIVO_ERROS) or not os.path.exists(ARQUIVO_DELTA):
        print("❌ Arquivos de erro ou de novas coordenadas não encontrados.")
        return

    # 1. Carregar os arquivos
    print("   Carregando bases de dados...")
    df_erros = pd.read_csv(ARQUIVO_ERROS, sep=";", dtype=str)
    df_delta = pd.read_csv(ARQUIVO_DELTA, sep=";", dtype=str)

    # 2. Identificar os CNES usando conjuntos (Sets) para alta performance
    cnes_esperados = set(df_erros["CNES"].dropna().unique())
    cnes_encontrados = set(df_delta["CNES"].dropna().unique())

    # 3. Fazer a diferença de conjuntos (Anti-Join)
    cnes_faltantes = cnes_esperados - cnes_encontrados

    qtd_esperados = len(cnes_esperados)
    qtd_encontrados = len(cnes_encontrados)
    qtd_faltantes = len(cnes_faltantes)

    print(f"   Total de unidades com erro (Esperado): {qtd_esperados}")
    print(f"   Total de unidades corrigidas (Encontrado): {qtd_encontrados}")
    print(f"   Total de unidades não encontradas (Falhas): {qtd_faltantes}")

    if qtd_faltantes == 0:
        print("✅ Sucesso total! Todas as unidades foram encontradas.")
        return

    # 4. Filtrar o dataframe de erros apenas com os CNES que faltaram
    df_falhas = df_erros[df_erros["CNES"].isin(cnes_faltantes)].copy()

    # 5. Trazer os dados de endereço do CSV principal para facilitar a análise manual
    if os.path.exists(ARQUIVO_PRINCIPAL):
        print("   Enriquecendo dados de endereço para facilitar sua análise...")
        df_main = pd.read_csv(ARQUIVO_PRINCIPAL, sep=";", dtype=str)

        # Define as colunas que queremos puxar (se existirem no seu CSV)
        colunas_endereco = [
            "ID_Municipio",
            "CNES",
            "Nome_Unidade",
            "Latitude",
            "Longitude",
            "Rua",
            "Numero",
            "Bairro",
        ]
        colunas_presentes = [col for col in colunas_endereco if col in df_main.columns]

        # Faz o Left Join para trazer os dados descritivos
        df_falhas = pd.merge(
            df_falhas, df_main[colunas_presentes], on="CNES", how="left"
        )

    # 6. Salvar o resultado
    df_falhas.to_csv(ARQUIVO_FALHAS, sep=";", index=False)
    print(
        f"\n✅ Análise concluída! Os {qtd_faltantes} registros não encontrados foram salvos em: {ARQUIVO_FALHAS}"
    )


if __name__ == "__main__":
    analisar_falhas()

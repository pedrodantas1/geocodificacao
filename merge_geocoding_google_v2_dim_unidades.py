import os

import pandas as pd

# CONFIGURAÇÕES
PASTA_TRATADOS = "Dados_Tratados"
ARQUIVO_PRINCIPAL = os.path.join(PASTA_TRATADOS, "Dim_Unidades_Saude.csv")
ARQUIVO_DELTA = "novas_coordenadas_google.csv"


def aplicar_atualizacoes():
    print("--- 🔄 INICIANDO MERGE DE COORDENADAS ---")

    if not os.path.exists(ARQUIVO_DELTA):
        print("❌ Arquivo de novas coordenadas não encontrado.")
        return

    # 1. Carregar Principal
    print("   Carregando arquivo principal...")
    df_main = pd.read_csv(ARQUIVO_PRINCIPAL, sep=";", dtype=str)
    qtd_antes = df_main["Latitude"].notna().sum()

    # 2. Carregar Delta (Novas coordenadas)
    print("   Carregando novas coordenadas...")
    df_delta = pd.read_csv(ARQUIVO_DELTA, sep=";", dtype=str)

    # 3. O Merge (Update)
    # Fazemos um Left Join do Principal com o Delta usando CNES
    df_merged = pd.merge(
        df_main,
        df_delta[["CNES", "Latitude_Nova", "Longitude_Nova"]],
        on="CNES",
        how="left",
    )

    # A Mágica: Preencher Latitude APENAS onde ela está vazia no principal E existe no Delta
    # Se já tem latitude no principal, NÃO mexe (mantém a original)
    df_merged["Latitude"] = df_merged["Latitude"].fillna(df_merged["Latitude_Nova"])
    df_merged["Longitude"] = df_merged["Longitude"].fillna(df_merged["Longitude_Nova"])

    # Remove colunas temporárias
    df_final = df_merged.drop(columns=["Latitude_Nova", "Longitude_Nova"])

    # Estatísticas
    qtd_depois = df_final["Latitude"].notna().sum()
    print(f"   Coordenadas antes: {qtd_antes}")
    print(f"   Coordenadas depois: {qtd_depois}")
    print(f"   ✅ Incremento de: {qtd_depois - qtd_antes} unidades.")

    # 4. Salvar (Sobrescreve o principal)
    df_final.to_csv(ARQUIVO_PRINCIPAL, sep=";", index=False)
    print(f"   💾 Arquivo principal atualizado com sucesso!")


if __name__ == "__main__":
    aplicar_atualizacoes()

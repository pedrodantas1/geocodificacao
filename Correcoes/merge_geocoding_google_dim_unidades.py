import os

import pandas as pd

# ============================
# CONFIGURAÇÕES
# ============================
PASTA_TRATADOS = "Dados_Tratados"
PASTA_TRABALHO = "Correcoes"
ARQUIVO_PRINCIPAL = os.path.join(PASTA_TRATADOS, "Dim_Unidades_Saude.csv")
ARQUIVO_DELTA = os.path.join(
    PASTA_TRABALHO, "novas_coordenadas_fallback_centroides.csv"
)


def aplicar_atualizacoes():
    print("--- 🔄 INICIANDO MERGE DE COORDENADAS (SOBRESCRITA) ---")

    if not os.path.exists(ARQUIVO_DELTA):
        print(f"❌ Arquivo de novas coordenadas ({ARQUIVO_DELTA}) não encontrado.")
        return

    # 1. Carregar Principal
    print("   Carregando arquivo principal...")
    df_main = pd.read_csv(ARQUIVO_PRINCIPAL, sep=";", dtype=str)
    qtd_antes = df_main["Latitude"].notna().sum()

    # 2. Carregar Delta (Novas coordenadas)
    print("   Carregando novas coordenadas...")
    df_delta = pd.read_csv(ARQUIVO_DELTA, sep=";", dtype=str)

    # 2.1 Remover duplicatas por segurança (mantém a última busca do Google)
    df_delta = df_delta.drop_duplicates(subset=["CNES"], keep="last")
    qtd_delta = len(df_delta)
    print(f"   Foram carregadas {qtd_delta} novas coordenadas para atualização.")

    # 3. O Merge (Update)
    # Fazemos um Left Join do Principal com o Delta usando CNES
    df_merged = pd.merge(
        df_main,
        df_delta[["CNES", "Latitude_Nova", "Longitude_Nova"]],
        on="CNES",
        how="left",
    )

    # A Mágica Invertida: Preenche primeiro com a NOVA.
    # Se a nova for nula (o CNES não estava no Delta), ele preenche com a ORIGINAL.
    df_merged["Latitude"] = df_merged["Latitude_Nova"].fillna(df_merged["Latitude"])
    df_merged["Longitude"] = df_merged["Longitude_Nova"].fillna(df_merged["Longitude"])

    # Remove colunas temporárias
    df_final = df_merged.drop(columns=["Latitude_Nova", "Longitude_Nova"])

    # Estatísticas
    qtd_depois = df_final["Latitude"].notna().sum()
    print(f"   Coordenadas válidas antes: {qtd_antes}")
    print(f"   Coordenadas válidas depois: {qtd_depois}")
    print(f"   ✅ Total de atualizações/inserções aplicadas: {qtd_delta}")

    # 4. Salvar (Sobrescreve o principal)
    df_final.to_csv(ARQUIVO_PRINCIPAL, sep=";", index=False)
    print(f"   💾 Arquivo principal atualizado com sucesso e pronto para o mapa!")


if __name__ == "__main__":
    aplicar_atualizacoes()

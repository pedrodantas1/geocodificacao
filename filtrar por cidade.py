import pandas as pd

# Caminho do arquivo original
input_file = "DENGBR25.csv"

# Arquivo de saída
output_file = "aracaju_sample.csv"

# Vamos trabalhar em chunks para lidar com arquivos grandes
chunksize = 100000  # 100 mil linhas por vez

# Criar o arquivo de saída vazio e escrever o cabeçalho apenas na primeira vez
first_chunk = True

for chunk in pd.read_csv(input_file, sep=",", dtype=str, chunksize=chunksize):
    # Filtrar apenas registros de Aracaju
    filtro = chunk[chunk["ID_MUNICIP"] == "280030"]
    
    if not filtro.empty:
        filtro.to_csv(output_file, mode="a", index=False, header=first_chunk)
        first_chunk = False

print("Arquivo filtrado gerado com sucesso:", output_file)

import os

from pyspark.sql import SparkSession
from itertools import groupby
from pyspark.sql.functions import col, when, lit, sum as _sum, collect_list, struct, regexp_replace, format_string
from pyspark.sql.types import StringType, BooleanType
import json

# Criar uma SparkSession
spark = SparkSession.builder.appName("EventosRecepcao").getOrCreate()

# Leitura dos CSVs
df_recepcao = spark.read.option("header", "true") \
                       .option("multiline", "true") \
                       .option("escape", "\"") \
                       .csv("recepcao_massivo.csv", header=True)
df_eventos = spark.read.option("header", "true") \
                       .option("multiline", "true") \
                       .option("escape", "\"") \
                       .csv("eventos_massivo.csv")

# Debug: Verificar esquemas e amostras dos DataFrames
# print("Schema df_recepcao:")
# df_recepcao.printSchema()
# print("Schema df_eventos:")
# df_eventos.printSchema()
# print("Amostra df_recepcao:")
# df_recepcao.show(5, truncate=False)
# print("Amostra df_eventos:")
# df_eventos.show(5, truncate=False)

# Conversão de vírgulas para pontos nas colunas de valores
print("Convertendo colunas de valores...")
df_recepcao = df_recepcao.withColumn(
    "recep_valor_tributo_retido",
    regexp_replace(col("recep_valor_tributo_retido"), ",", ".").cast("double")
).withColumn(
    "recep_valor_rendimento_bruto",
    regexp_replace(col("recep_valor_rendimento_bruto"), ",", ".").cast("double")
)

# Debug: Verificar após a conversão
# print("Amostra df_recepcao após conversão:")
# df_recepcao.show(5, truncate=False)

# Somatórios para cada id_evento
print("Calculando somatórios...")
df_recepcao_agg = df_recepcao.groupBy("id_evento", "id_evento_new").agg(
    _sum("recep_valor_rendimento_bruto").alias("valor_base_calculo"),
    _sum("recep_valor_tributo_retido").alias("valor_tributo_retido"),
    collect_list(
        struct(
            "natureza_rendimento",
            "data_fato_gerador",
            "tipo_tributacao",
            "tipo_pessoa",
            "codigo_tributo",
            "codigo_recolhimento",
            "numero_inscricao",
            "recep_valor_rendimento_bruto",
            "recep_valor_tributo_retido"
        )
    ).alias("dados_recepcao")
)

# Debug: Verificar dados agregados
# print("Amostra df_recepcao_agg:")
# df_recepcao_agg.show(5, truncate=False)

# Join para combinar os DataFrames
print("Realizando join dos DataFrames...")
joined_df = df_eventos.join(df_recepcao_agg, "id_evento", "left")

# Debug: Verificar resultado do join
# print("Amostra joined_df:")
# joined_df.show(5, truncate=False)

# Função para modificar json_evento e gerar query_sql
def process_event(row):
    print(f"Processando id_evento: {row.id_evento}")
    json_evento = json.loads(row.json_evento)
    dados_recepcao = row.dados_recepcao

    # Apagar os objetos de dados_rendimento
    json_evento['dados_estabelecimento']['dados_beneficiario']['dados_rendimento'] = []

    # Agrupamento e lógica para preencher novos dados_rendimento
    novos_dados_rendimento = []
    for natureza, group_items in groupby(sorted(dados_recepcao, key=lambda x: x.natureza_rendimento), lambda x: x.natureza_rendimento):
        dados_pagamento = []

        for key, group in groupby(sorted(group_items, key=lambda x: (x.data_fato_gerador, x.numero_inscricao, x.tipo_tributacao, x.tipo_pessoa, x.codigo_tributo, x.codigo_recolhimento)),
                                  lambda x: (x.data_fato_gerador, x.numero_inscricao, x.tipo_tributacao, x.tipo_pessoa, x.codigo_tributo, x.codigo_recolhimento)):
            group = list(group)
            if key[4] == "1":  # codigo_tributo == "1"
                if key[5] in ("6800", "6813", "1605"):  # Verificar codigo_recolhimento
                    if key[2] == "I":
                        dados_pagamento.append({
                            "data_fato_gerador": key[0],
                            "indicativo_fci_ou_scp": "1",
                            "valor_rendimento_bruto": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ","),
                            "numero_inscricao_fci_ou_scp": key[1]
                        })
                    elif key[2] == "T":
                        if key[3] == "J":
                            dados_pagamento.append({
                                "retencoes_fonte": {
                                    "valor_imposto_renda": f"{sum(item.recep_valor_tributo_retido for item in group):.2f}".replace(".", ","),
                                    "valor_base_imposto_renda": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ",")
                                },
                                "data_fato_gerador": key[0],
                                "indicativo_fci_ou_scp": "1",
                                "valor_rendimento_bruto": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ","),
                                "numero_inscricao_fci_ou_scp": key[1]
                            })
                        elif key[3] == "F":
                            dados_pagamento.append({
                                "data_fato_gerador": key[0],
                                "indicativo_fci_ou_scp": "1",
                                "valor_rendimento_bruto": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ","),
                                "numero_inscricao_fci_ou_scp": key[1],
                                "valor_retencao_imposto_renda": f"{sum(item.recep_valor_tributo_retido for item in group):.2f}".replace(".", ","),
                                "valor_rendimento_tributavel": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ",")
                            })
                else:  # codigo_recolhimento not in ("6800", "6813", "1605")
                    if key[2] == "I":
                        dados_pagamento.append({
                            "data_fato_gerador": key[0],
                            "valor_rendimento_bruto": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ",")
                        })
                    elif key[2] == "T" and key[3] == "J":
                        dados_pagamento.append({
                            "retencoes_fonte": {
                                "valor_imposto_renda": f"{sum(item.recep_valor_tributo_retido for item in group):.2f}".replace(".", ","),
                                "valor_base_imposto_renda": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ",")
                            },
                            "data_fato_gerador": key[0],
                            "valor_rendimento_bruto": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ",")
                        })
                    elif key[2] == "T" and key[3] == "F":
                        dados_pagamento.append({
                            "data_fato_gerador": key[0],
                            "valor_rendimento_bruto": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ","),
                            "valor_retencao_imposto_renda": f"{sum(item.recep_valor_tributo_retido for item in group):.2f}".replace(".", ","),
                            "valor_rendimento_tributavel": f"{sum(item.recep_valor_rendimento_bruto for item in group):.2f}".replace(".", ",")
                        })

        novos_dados_rendimento.append({
            "natureza_rendimento": natureza,
            "dados_pagamento": dados_pagamento
        })

    json_evento['dados_estabelecimento']['dados_beneficiario']['dados_rendimento'] = novos_dados_rendimento

    # Modificar a chave identificacao_unica_evento se necessário
    if row.status_evento in ('EX', 'EG'):
        json_evento['identificacao_unica_evento'] = row.id_evento_new

    # Criar query_sql
    query = None
    if row.status_evento == 'NO':
        query = f"UPDATE tabela SET json_evento = '{json.dumps(json_evento)}', valor_base_calculo = '{f'{row.valor_base_calculo:.2f}'.replace('.', ',')}', valor_tributo_retido = '{f'{row.valor_tributo_retido:.2f}'.replace('.', ',')}' WHERE id_evento = '{row.id_evento}'"
    elif row.status_evento in ('EX', 'EG'):
        query = f"INSERT INTO tabela (json_evento, id_evento, status_evento, valor_base_calculo, valor_tributo_retido) VALUES ('{json.dumps(json_evento)}', '{row.id_evento_new}', 'NO', '{f'{row.valor_base_calculo:.2f}'.replace('.', ',')}', '{f'{row.valor_tributo_retido:.2f}'.replace('.', ',')}')"

    # Verificar exceções
    exception_flag = any(item.codigo_tributo != "1" or item.tipo_tributacao not in ["T", "I"] for item in dados_recepcao)

    return row.id_evento, query, exception_flag

# Processar os eventos
print("Aplicando processamento nos eventos...")
processed_rdd = joined_df.rdd.mapPartitions(lambda partition: (process_event(row) for row in partition))
processed_df = processed_rdd.toDF(["id_evento", "query_sql", "exception_flag"])

# Salvar somente a coluna query_sql em um arquivo .txt
output_path = "C:/Users/fabio/Desktop/PythonProjects/pythonProject1/saida/processed_queries.txt"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w", encoding="utf-8") as f:
    for row in processed_df.select("query_sql").collect():
        f.write(row.query_sql + "\n")

print(f"Arquivo de queries salvo em: {output_path}")

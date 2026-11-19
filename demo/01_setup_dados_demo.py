# Databricks notebook source
# MAGIC %md
# MAGIC # Setup da demo: SAS MERGE → PySpark com Genie Code
# MAGIC
# MAGIC Cria 4 tabelas pequenas e **sintéticas** do ACME Bank (banco fictício) no Unity Catalog:
# MAGIC
# MAGIC | Tabela | Papel | Equivale no SAS a |
# MAGIC |---|---|---|
# MAGIC | `clientes` | cadastro | `CLI.CLIENTES` |
# MAGIC | `saldo_cc` | saldo médio mensal e segmento atualizado | `CC.SALDO_CC` |
# MAGIC | `posicao_invest` | clientes com investimento | `INV.POSICAO_INVEST` |
# MAGIC | `publico_campanha_sas_ref` | saída gerada pelo SAS (referência para batimento) | `WORK.PUBLICO_CAMPANHA` |
# MAGIC
# MAGIC Os dados contêm, de propósito, os casos de borda que a skill ensina a tratar:
# MAGIC - **C002**: segmento muda entre cadastro (`VAREJO`) e saldo (`PRIME`). No SAS, vale o dataset listado por último.
# MAGIC - **C003**: segmento **missing** no saldo. No SAS, o missing sobrescreve o cadastro, e um `coalesce` ingênuo erra.
# MAGIC - **C005**: cliente sem linha de saldo. `IF VL_SALDO_MEDIO < 50000` **mantém** a linha no SAS, e um filtro ingênuo no Spark descarta.
# MAGIC - **C009**: saldo sem cadastro. Sai no `IF A`.
# MAGIC - **C010**: investimento sem cadastro. Não tem efeito.

# COMMAND ----------

dbutils.widgets.text("catalogo", "main", "Catálogo")
dbutils.widgets.text("schema", "demo_sas_genie", "Schema")
catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalogo}`.`{schema}`")
destino = f"`{catalogo}`.`{schema}`"
print(f"Tabelas serão criadas em {destino}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

clientes = spark.createDataFrame(
    [
        ("C001", "Ana Souza",      "VAREJO",  "SP",  3000.0),
        ("C002", "Bruno Lima",     "VAREJO",  "RJ",  4500.0),
        ("C003", "Carla Mendes",   "PRIME",   "MG", 12000.0),
        ("C004", "Diego Rocha",    "PRIME",   "SP", 15000.0),
        ("C005", "Elisa Martins",  "VAREJO",  "BA",  2500.0),
        ("C006", "Fabio Nunes",    "PRIVATE", "SP", 60000.0),
        ("C007", "Gabriela Alves", "VAREJO",  "PR",  3800.0),
        ("C008", "Hugo Pereira",   "PRIME",   "RS",  9000.0),
    ],
    StructType([
        StructField("cd_cliente", StringType()),
        StructField("nm_cliente", StringType()),
        StructField("segmento", StringType()),
        StructField("uf", StringType()),            # descartada pelo KEEP= no SAS
        StructField("renda_mensal", DoubleType()),
    ]),
)

saldo_cc = spark.createDataFrame(
    [
        ("C001",   1200.0, "VAREJO"),
        ("C002",   8000.0, "PRIME"),
        ("C003",  25000.0, None),
        ("C004",  30000.0, "PRIME"),
        ("C006", 120000.0, "PRIVATE"),
        ("C007",    900.0, "VAREJO"),
        ("C009",   5000.0, "VAREJO"),
    ],
    StructType([
        StructField("cd_cliente", StringType()),
        StructField("vl_saldo_medio", DoubleType()),
        StructField("segmento", StringType()),
    ]),
)

posicao_invest = spark.createDataFrame(
    [
        ("C004", "CDB",       10000.0),
        ("C008", "LCI",        5000.0),
        ("C010", "TESOURO",    2000.0),
    ],
    StructType([
        StructField("cd_cliente", StringType()),
        StructField("tp_produto", StringType()),
        StructField("vl_aplicado", DoubleType()),
    ]),
)

# Saída do SAS exportada para comparação (resultado esperado do script campanha_investimento.sas)
publico_campanha_sas_ref = spark.createDataFrame(
    [
        ("C001", "Ana Souza",      "VAREJO", 3000.0,  1200.0),
        ("C002", "Bruno Lima",     "PRIME",  4500.0,  8000.0),
        ("C003", "Carla Mendes",   None,    12000.0, 25000.0),
        ("C005", "Elisa Martins",  "VAREJO", 2500.0,  None),
        ("C007", "Gabriela Alves", "VAREJO", 3800.0,   900.0),
    ],
    StructType([
        StructField("cd_cliente", StringType()),
        StructField("nm_cliente", StringType()),
        StructField("segmento", StringType()),
        StructField("renda_mensal", DoubleType()),
        StructField("vl_saldo_medio", DoubleType()),
    ]),
)

for nome, df in [
    ("clientes", clientes),
    ("saldo_cc", saldo_cc),
    ("posicao_invest", posicao_invest),
    ("publico_campanha_sas_ref", publico_campanha_sas_ref),
]:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{destino}.{nome}")
    print(f"{destino}.{nome}: {df.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza (executar ao final da demo)
# MAGIC ```python
# MAGIC spark.sql(f"DROP SCHEMA IF EXISTS `{catalogo}`.`{schema}` CASCADE")
# MAGIC ```

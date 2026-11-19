# Exemplo resolvido: público de campanha de cross-sell de investimentos

Cenário: todo mês, a área de CRM do ACME Bank (banco fictício) gera em SAS a lista de clientes **sem investimento**
e com **saldo médio em conta abaixo de R$ 50 mil**. Acima desse valor, o cliente vai
para a assessoria.

## SAS de origem

```sas
PROC SORT DATA=CLI.CLIENTES OUT=WORK.CLIENTES; BY CD_CLIENTE; RUN;
PROC SORT DATA=CC.SALDO_CC OUT=WORK.SALDO_CC; BY CD_CLIENTE; RUN;
PROC SORT DATA=INV.POSICAO_INVEST OUT=WORK.POSICAO_INVEST; BY CD_CLIENTE; RUN;

DATA WORK.BASE_CLIENTE;
  MERGE WORK.CLIENTES (IN=A KEEP=CD_CLIENTE NM_CLIENTE SEGMENTO RENDA_MENSAL)
        WORK.SALDO_CC (IN=B KEEP=CD_CLIENTE VL_SALDO_MEDIO SEGMENTO);
  BY CD_CLIENTE;
  IF A;
RUN;

DATA WORK.PUBLICO_CAMPANHA;
  MERGE WORK.BASE_CLIENTE (IN=A)
        WORK.POSICAO_INVEST (IN=B KEEP=CD_CLIENTE);
  BY CD_CLIENTE;
  IF A AND NOT B;
  IF VL_SALDO_MEDIO < 50000;
RUN;
```

## Inventário

| Item | Passo 1 | Passo 2 |
|---|---|---|
| Datasets | A = `clientes`, B = `saldo_cc` | A = `base_cliente`, B = `posicao_invest` |
| BY | `cd_cliente` | `cd_cliente` |
| Colunas sobrepostas | `segmento` | nenhuma |
| Subconjunto | `IF A` → `left` | `IF A AND NOT B` → `left_anti` |
| Filtro adicional | - | `vl_saldo_medio < 50000` (missing passa no SAS) |

## Armadilhas presentes

1. **`segmento` existe nos dois lados.** Para C003 o saldo traz segmento missing, e o
   SAS grava missing. `coalesce(b.segmento, a.segmento)` gravaria `PRIME`, o que está errado.
2. **C005 não tem saldo.** No SAS, `missing < 50000` é verdadeiro, então o cliente entra
   na campanha. `F.col("vl_saldo_medio") < 50000` descartaria o cliente. Pergunta para o
   negócio: cliente sem saldo apurado deve mesmo receber a campanha?
3. **Os PROC SORT** existem só para o MERGE e são removidos.

## PySpark convertido

```python
from pyspark.sql import functions as F

catalogo, schema = "main", "demo_sas_genie"
t = lambda nome: spark.table(f"{catalogo}.{schema}.{nome}")

# SAS: PROC SORT ... BY CD_CLIENTE  -> removido (join não depende de ordenação)

# SAS: MERGE CLIENTES (IN=A KEEP=...) SALDO_CC (IN=B KEEP=...); BY CD_CLIENTE; IF A;
a = t("clientes").select("cd_cliente", "nm_cliente", "segmento", "renda_mensal").alias("a")
b = t("saldo_cc").select("cd_cliente", "vl_saldo_medio", "segmento").alias("b")

base_cliente = (
    a.join(b, F.col("a.cd_cliente") == F.col("b.cd_cliente"), "left")
     .select(
         F.col("a.cd_cliente").alias("cd_cliente"),
         F.col("a.nm_cliente").alias("nm_cliente"),
         # SAS: coluna sobreposta -> vale B (último no MERGE) quando a chave existe em B, mesmo missing
         F.when(F.col("b.cd_cliente").isNotNull(), F.col("b.segmento"))
          .otherwise(F.col("a.segmento")).alias("segmento"),
         F.col("a.renda_mensal").alias("renda_mensal"),
         F.col("b.vl_saldo_medio").alias("vl_saldo_medio"),
     )
)

# SAS: MERGE BASE_CLIENTE (IN=A) POSICAO_INVEST (IN=B KEEP=CD_CLIENTE); IF A AND NOT B;
invest = t("posicao_invest").select("cd_cliente")
publico_campanha = (
    base_cliente.join(invest, "cd_cliente", "left_anti")
    # SAS: IF VL_SALDO_MEDIO < 50000;  (missing numérico é menor que qualquer número no SAS)
    .filter(F.col("vl_saldo_medio").isNull() | (F.col("vl_saldo_medio") < 50000))
)
```

## Validação esperada

| Verificação | Resultado |
|---|---|
| Chaves únicas em `clientes`, `saldo_cc`, `posicao_invest` | sim (1:1) |
| Passo 1, distribuição: só A / só B / A e B | 2 / 1 / 6 |
| Linhas finais | 5 (C001, C002, C003, C005, C007) |
| `exceptAll` contra `publico_campanha_sas_ref`, nos dois sentidos | 0 e 0 |
| Conversão ingênua (`coalesce` + filtro sem `isNull`) | 4 linhas e 2 divergências (C003, C005) |

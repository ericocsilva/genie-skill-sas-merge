---
name: sas-merge-pyspark
description: >
  Converte DATA steps SAS que usam MERGE ... BY com flags IN= (ex.: IF A; IF A AND B;
  IF A AND NOT B) para PySpark no Databricks, preservando a semântica exata do SAS e
  validando o resultado contra as tabelas no Unity Catalog. Use quando o usuário colar
  ou referenciar um DATA step com MERGE, perguntar "como fica esse MERGE em PySpark",
  ou pedir para comparar a saída convertida com a saída do SAS. Não use para outros
  construtos SAS (PROC SQL, RETAIN, arrays, hash objects, macros).
---

# SAS MERGE → PySpark (skill didática)

Esta skill ensina o Genie Code a converter **um único construto SAS**: o DATA step
`MERGE` com `BY` e flags `IN=`. O escopo é propositalmente pequeno. O foco é mostrar
como o Genie lê o código SAS, consulta as tabelas reais, gera o PySpark, executa e
prova a paridade com o SAS.

Um exemplo completo, baseado em um cenário bancário real, está em
[exemplo_cross_sell.md](exemplo_cross_sell.md).

---

## 1. Verificar o escopo antes de começar

**Dentro do escopo**
- `DATA saida; MERGE ds1 (IN=a ...) ds2 (IN=b ...); BY chave(s); IF ...; RUN;`
- Opções de dataset `KEEP=`, `DROP=`, `RENAME=` e `WHERE=` nos datasets do MERGE.
- Instruções `IF` de subconjunto logo após o MERGE (sobre flags e sobre colunas).
- `PROC SORT ... BY` que existe apenas para preparar o MERGE.

**Fora do escopo: pare e informe o usuário**
- MERGE sem `BY` (merge posicional).
- `RETAIN`, `ARRAY`, `DO` loops, `LAG`, hash objects, `OUTPUT` múltiplo.
- `FIRST.` / `LAST.`, macros (`%MACRO`, `&var`) que geram o próprio MERGE.
- Relação **muitos-para-muitos** nas chaves do `BY` (ver passo 3).

Quando algo estiver fora do escopo, responda em uma frase qual construto não é
coberto e converta apenas a parte que é coberta, deixando um comentário `# TODO SAS:`
no ponto pendente. Não invente equivalência.

---

## 2. Inventariar o DATA step

Antes de escrever código, monte e mostre ao usuário uma tabela curta:

| Item | Valor |
|---|---|
| Datasets, na ordem do MERGE | ex.: `A = clientes`, `B = saldo_cc` |
| Flags `IN=` | ex.: `a`, `b` |
| Chave(s) do `BY` | ex.: `cd_cliente` |
| Colunas mantidas por dataset (`KEEP=`/`DROP=`/`RENAME=`) | listar |
| Colunas não-chave **presentes em mais de um dataset** | listar (ver passo 5) |
| Condição de subconjunto | ex.: `IF a AND NOT b` |
| Filtros adicionais | ex.: `IF vl_saldo_medio < 50000` |

Mapeie cada dataset SAS para a tabela do Unity Catalog indicada pelo usuário. Se o
nome não for informado, pergunte. Não presuma.

---

## 3. Checar a cardinalidade das chaves nos dados reais

O MERGE do SAS **não** é um join relacional quando há chaves repetidas dos dois lados.
Execute, para cada dataset do MERGE:

```sql
SELECT COUNT(*) AS linhas, COUNT(DISTINCT <chaves>) AS chaves_distintas
FROM <tabela>
```

| Resultado | Ação |
|---|---|
| Chave única em todos os datasets (1:1) | Seguir |
| Chave repetida em apenas um dataset (1:N) **e** sem colunas sobrepostas | Seguir |
| Chave repetida em apenas um dataset (1:N) **com** colunas sobrepostas | Parar e avisar: no SAS, a 2ª linha em diante do grupo recebe o valor do dataset que tem várias linhas, não o do outro. Pedir confirmação antes de seguir |
| Chave repetida nos dois datasets (N:M) | Parar: fora do escopo. O SAS pareia linha a linha dentro do grupo, e um join faria produto cartesiano |

Mostre o resultado da checagem ao usuário.

---

## 4. Traduzir a condição IN= para o tipo de join

| Condição SAS após o MERGE | Join no Spark |
|---|---|
| sem `IF` sobre flags, ou `IF a OR b` | `full` (FULL OUTER) |
| `IF a` | `left` |
| `IF b` | `right` |
| `IF a AND b` | `inner` |
| `IF a AND NOT b` | `left_anti` (A sem correspondência em B) |
| `IF b AND NOT a` | `left_anti` com B à esquerda |
| `IF NOT (a AND b)` ou outras combinações | `full` + colunas de flag + `filter` |

Regra fixa: **MERGE sem IF não vira INNER JOIN.** É o erro de conversão mais comum.

Quando a condição não estiver na tabela, use o padrão canônico, que é fiel por construção:

```python
a = df_a.withColumn("_in_a", F.lit(True)).alias("a")
b = df_b.withColumn("_in_b", F.lit(True)).alias("b")
j = a.join(b, F.col("a.chave") == F.col("b.chave"), "full")
in_a = F.col("a._in_a").isNotNull()
in_b = F.col("b._in_b").isNotNull()
j = j.filter(<condição SAS reescrita com in_a / in_b>)
```

Em joins `full` ou `right`, a chave de saída é `F.coalesce(a.chave, b.chave)`.

---

## 5. Colunas não-chave presentes nos dois datasets

No SAS, quando a chave existe nos dois datasets, **vale o valor do dataset listado por
último no MERGE, mesmo que esse valor seja missing**. Quando a chave existe só em um
lado, vale o valor desse lado.

```python
F.when(F.col("b.chave").isNotNull(), F.col("b.col")).otherwise(F.col("a.col")).alias("col")
```

Não use `F.coalesce(b.col, a.col)`. Ele troca um missing legítimo de B pelo valor de A,
o que diverge do SAS. Quando encontrar esse caso, explique a diferença ao usuário.

---

## 6. Valores missing nos IFs seguintes

| Situação | SAS | Spark | Como preservar o SAS |
|---|---|---|---|
| `IF x < 100` com `x` missing | missing é **menor** que qualquer número: linha **mantida** | `null < 100` é null: linha **descartada** | `F.col("x").isNull() \| (F.col("x") < 100)` |
| `IF x > 100` / `x >= 100` com `x` missing | linha descartada | linha descartada | nada a fazer |
| `IF c = ' '` (caractere) | missing de texto é branco | `null` ≠ `''` | `F.trim(F.coalesce(F.col("c"), F.lit(""))) == ""` |
| Chave do `BY` missing nos dois lados | as chaves se casam | `null = null` não casa | usar `eqNullSafe` na condição de join |

Sempre que uma dessas linhas se aplicar, aponte ao usuário se o comportamento do SAS
parece intencional para o negócio ou se parece um efeito colateral. Mantenha o
comportamento do SAS no código e deixe a decisão com o usuário.

---

## 7. Escrever o código

- Remova os `PROC SORT` que só preparam o MERGE. O Spark não precisa de ordenação para join.
  Diga ao usuário que foram removidos e por quê.
- Use PySpark DataFrame API, com `alias` em cada dataset.
- Aplique `KEEP=`/`DROP=`/`WHERE=` **antes** do join (poda de colunas e linhas).
- Projete as colunas explicitamente no `select`. Não use `select("*")` depois do join.
- Ordem das colunas na saída: a mesma do SAS, ou seja, a ordem em que cada coluna aparece
  pela primeira vez percorrendo os datasets na ordem do MERGE.
- Comente cada bloco com a linha SAS de origem (`# SAS: IF a AND NOT b;`).
- Leia as tabelas com `spark.table("<catalogo>.<schema>.<tabela>")`.

---

## 8. Executar e provar a paridade

Execute o código no notebook e mostre:

1. **Distribuição das flags**, antes do filtro de subconjunto: quantidade de linhas
   `só A`, `só B` e `A e B`.
2. **Contagem final** de linhas.
3. **Comparação com a saída do SAS**, se o usuário indicar uma tabela de referência:
   ```python
   cols = ref.columns
   so_spark = convertido.select(cols).exceptAll(ref.select(cols))
   so_sas   = ref.select(cols).exceptAll(convertido.select(cols))
   ```
   Paridade é `so_spark.count() == 0` e `so_sas.count() == 0`, e a ordem das colunas igual.
   Se houver diferença, mostre as linhas divergentes e relacione a causa a um dos passos 3 a 6.

---

## 9. Formato da resposta final

1. Tabela de inventário (passo 2).
2. Resultado da checagem de chaves (passo 3).
3. Tabela "linha SAS → PySpark" com uma linha por instrução SAS.
4. Diferenças de semântica encontradas (passos 5 e 6), cada uma em uma frase.
5. Resultado da validação (passo 8).

# Roteiro da demo (cerca de 15 minutos)

**Mensagem central:** o Genie Code não só traduz sintaxe. Com uma skill pequena, ele
consulta os dados reais, encontra diferenças de semântica entre SAS e Spark, executa o
código e prova a paridade com a saída do SAS.

## Preparação (antes da reunião)

1. Importar `01_setup_dados_demo.py` no workspace, definir `catalogo` e `schema` e executar (dados do ACME Bank, banco fictício).
2. Copiar a pasta `sas-merge-pyspark/` para `/Users/<seu-usuario>/.assistant/skills/`.
3. Abrir um notebook novo, anexar um serverless e abrir o painel do Genie Code em modo **Agent**.
4. Ter `campanha_investimento.sas` aberto para copiar e colar.

## Ato 1: sem a skill (2 min, opcional)

Desative a skill (ou use um workspace sem ela) e peça:

> Converta este SAS para PySpark: *(colar o script)*

Resultado típico: conversão sintaticamente correta, mas com `coalesce` na coluna
`segmento` e/ou filtro `vl_saldo_medio < 50000` sem tratar null. Guarde esse código
para comparar no Ato 3.

## Ato 2: com a skill (8 min)

**Prompt 1: conversão guiada**
> @sas-merge-pyspark Converta o SAS abaixo. As tabelas estão em `<catalogo>.<schema>`:
> CLI.CLIENTES = clientes, CC.SALDO_CC = saldo_cc, INV.POSICAO_INVEST = posicao_invest.
> *(colar o script)*

O que destacar enquanto o Genie trabalha:
- Monta o **inventário** do MERGE antes de escrever código.
- **Consulta as tabelas** para checar a unicidade das chaves (1:1).
- Explica por que os **PROC SORT foram removidos**.
- Aponta as duas armadilhas: `segmento` sobreposto (C003) e `missing < 50000` (C005).
- Devolve a pergunta para o negócio sobre o C005, em vez de decidir sozinho.

**Prompt 2: prova de paridade**
> Compare o resultado com a saída do SAS em `<catalogo>.<schema>.publico_campanha_sas_ref`.

Esperado: 5 linhas, 0 divergências nos dois sentidos, ordem de colunas igual.

## Ato 3: o valor da validação (3 min)

**Prompt 3**
> Execute também a versão abaixo e compare com a referência do SAS. Explique as diferenças.
> *(colar o código do Ato 1, ou a "conversão ingênua" abaixo)*

```python
from pyspark.sql import functions as F
t = lambda n: spark.table(f"<catalogo>.<schema>.{n}")
a, b = t("clientes").alias("a"), t("saldo_cc").alias("b")
ingenua = (a.join(b, "cd_cliente", "left")
    .select("cd_cliente", "a.nm_cliente", F.coalesce("b.segmento", "a.segmento").alias("segmento"),
            "a.renda_mensal", "b.vl_saldo_medio")
    .join(t("posicao_invest").select("cd_cliente"), "cd_cliente", "left_anti")
    .filter(F.col("vl_saldo_medio") < 50000))
```

Esperado: 4 linhas. O Genie identifica que C005 sumiu (filtro com null) e que C003 ficou
com `PRIME` em vez de missing (`coalesce`). Em uma base real, são clientes fora da
campanha ou segmentados errado, e o código "parece certo".

## Ato 4: a skill sabe o próprio limite (2 min)

**Prompt 4**
> @sas-merge-pyspark Converta também:
> ```sas
> DATA WORK.ULTIMO_SALDO;
>   SET WORK.SALDO_HIST;
>   BY CD_CLIENTE;
>   RETAIN VL_ANTERIOR;
>   IF LAST.CD_CLIENTE;
> RUN;
> ```

Esperado: o Genie informa que `RETAIN` e `LAST.` estão fora do escopo da skill, não
inventa uma equivalência e deixa um `# TODO SAS:` no código.

## Fechamento

- Uma skill com menos de 200 linhas, escrita uma vez, vira padrão para toda a equipe
  quando publicada em `/Workspace/.assistant/skills/`.
- A mesma abordagem se estende a outros construtos (`FIRST./LAST.`, `PROC TRANSPOSE`,
  `INTNX`/`INTCK`), cada um como uma skill pequena e testável.

## Limpeza

Executar a célula de limpeza no final de `01_setup_dados_demo.py`.

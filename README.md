# Genie Code skill de exemplo: SAS MERGE → PySpark

Skill didática e de escopo fechado para mostrar como skills do Genie Code apoiam uma
migração SAS → Databricks. Cobre **um** construto SAS, o DATA step `MERGE ... BY` com
flags `IN=`, aplicado a um cenário bancário comum no ACME Bank, um banco fictício: o
público de uma campanha de cross-sell de investimentos.

## Conteúdo

```
genie-skill-sas-merge/
├── sas-merge-pyspark/            ← a skill (copiar esta pasta)
│   ├── SKILL.md                  instruções que o Genie Code segue
│   └── exemplo_cross_sell.md     exemplo resolvido (gabarito)
└── demo/
    ├── 01_setup_dados_demo.py    notebook: cria as tabelas sintéticas no UC
    ├── campanha_investimento.sas script SAS de origem da demo
    └── roteiro_demo.md           roteiro com prompts e resultados esperados
```

## Instalação

1. Importar `demo/01_setup_dados_demo.py` no workspace e executar (widgets `catalogo` e `schema`).
2. Copiar `sas-merge-pyspark/` para um destes locais:
   - `/Users/<usuario>/.assistant/skills/`: só para você (recomendado para a demo);
   - `/Workspace/.assistant/skills/`: para todo o workspace.
3. Abrir um **chat novo** do Genie Code em modo Agent. Edições na skill só valem em chats novos.
4. Invocar com `@sas-merge-pyspark` ou deixar o Genie carregar a skill pela descrição.

Referência: https://docs.databricks.com/aws/en/genie-code/skills

## O que a skill ensina

| Regra | Por que importa |
|---|---|
| Condição `IN=` → tipo de join | `MERGE` sem `IF` é FULL OUTER, não INNER |
| Checagem de cardinalidade nos dados reais | N:M no SAS não é join; o Genie consulta as tabelas antes |
| Coluna sobreposta: vale o último dataset, mesmo missing | `coalesce` ingênuo troca valores |
| `missing < valor` é verdadeiro no SAS | filtro ingênuo no Spark descarta linhas |
| Prova de paridade com `exceptAll` contra a saída do SAS | o resultado é demonstrado, não presumido |
| Limite explícito de escopo | o Genie recusa o que não sabe, em vez de inventar |

## Validação feita

O notebook de setup e o PySpark do gabarito foram executados localmente (PySpark 4.1).
- Gabarito: 5 linhas, 0 divergências nos dois sentidos contra `publico_campanha_sas_ref`,
  ordem de colunas igual.
- Conversão ingênua do roteiro: 4 linhas, com divergências em C003 e C005, como esperado.

A saída de referência foi derivada manualmente da semântica do SAS, sem executar em um
ambiente SAS. Se houver acesso a SAS, recomenda-se rodar `campanha_investimento.sas`
sobre os mesmos dados para confirmar.

## Origem dos dados

ACME Bank é um banco fictício. Todos os nomes, clientes e valores são sintéticos e
foram criados apenas para esta demo. O conteúdo da skill se baseia na semântica
pública documentada de SAS e Apache Spark.

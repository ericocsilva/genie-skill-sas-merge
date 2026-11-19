/*============================================================================*/
/* ACME BANK - CAMPANHA CROSS-SELL INVESTIMENTOS - PUBLICO ALVO MENSAL        */
/*                                                                            */
/* Objetivo: listar clientes da base cadastral que NAO possuem investimento   */
/* e cujo saldo medio em conta corrente esta abaixo de 50 mil (acima disso o  */
/* cliente e atendido pela assessoria, fora da campanha).                     */
/*                                                                            */
/* Entradas : CLI.CLIENTES        - cadastro de clientes                      */
/*            CC.SALDO_CC         - saldo medio mensal e segmento atualizado  */
/*            INV.POSICAO_INVEST  - posicao de investimentos                  */
/* Saida    : WORK.PUBLICO_CAMPANHA                                           */
/*============================================================================*/

PROC SORT DATA=CLI.CLIENTES OUT=WORK.CLIENTES;
  BY CD_CLIENTE;
RUN;

PROC SORT DATA=CC.SALDO_CC OUT=WORK.SALDO_CC;
  BY CD_CLIENTE;
RUN;

PROC SORT DATA=INV.POSICAO_INVEST OUT=WORK.POSICAO_INVEST;
  BY CD_CLIENTE;
RUN;

/* Passo 1: base cadastral enriquecida com saldo e segmento atualizado */
DATA WORK.BASE_CLIENTE;
  MERGE WORK.CLIENTES (IN=A KEEP=CD_CLIENTE NM_CLIENTE SEGMENTO RENDA_MENSAL)
        WORK.SALDO_CC (IN=B KEEP=CD_CLIENTE VL_SALDO_MEDIO SEGMENTO);
  BY CD_CLIENTE;
  IF A;
RUN;

/* Passo 2: remove quem ja investe e aplica corte de saldo */
DATA WORK.PUBLICO_CAMPANHA;
  MERGE WORK.BASE_CLIENTE (IN=A)
        WORK.POSICAO_INVEST (IN=B KEEP=CD_CLIENTE);
  BY CD_CLIENTE;
  IF A AND NOT B;
  IF VL_SALDO_MEDIO < 50000;
RUN;

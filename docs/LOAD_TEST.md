# Relatório de Teste de Carga (`bun run test:load`)

## 1. Ambiente de Execução

- **Runtime**: Bun `1.4.4` (`linux-x64`)
- **Banco de Dados**: PostgreSQL `17-alpine` em Docker (porta `5433`, `DB_POOL_MAX=30`, isolamento `READ COMMITTED`, `lock_timeout=5000ms`, `statement_timeout=10000ms`)
- **Mensageria**: LocalStack `3.8` (AWS SQS FIFO em Docker, porta `4566`)
- **Workers ativos durante o experimento**: `OutboxPublisherWorker` (`OUTBOX_POLL_INTERVAL_MS=100ms`) + `PendingReferenceWorker`

## 2. Metodologia

O script [`test/load/load-test.ts`](../test/load/load-test.ts) (`bun run test:load`) executa o seguinte cenário:
1. Cria **25 wallets independentes**, cada uma com saldo inicial de `1000.00 BRL` (gerando 25 transações `OPENING`, 25 entradas de ledger e 25 eventos `WalletBalanceChanged`).
2. Dispara **500 transações financeiras reais** (`BET`, `WIN` e `LOSS`) distribuídas entre as 25 wallets (20 operações por wallet) com **25 workers HTTP concorrentes** (`concurrency = 25`), exercitando simultaneamente:
   - paralelismo entre wallets distintas;
   - contenção real de lock pessimista (`SELECT ... FOR UPDATE`) quando múltiplos workers atingem a mesma wallet;
   - validação do constraint trigger deferrable `trg_wallet_ledger_consistency` no commit de cada transação;
   - gravação e publicação concorrente da Transactional Outbox para o SQS FIFO.
3. Ao final da carga, executa `assertWalletConsistent(orm, walletId)` nas 25 wallets, validando que `wallet.balance == Σ(ledger)` e que todas as versões e entradas de ledger são contíguas e balanceadas.

## 3. Resultados Obtidos

```json
{
  "totalRequests": 500,
  "wallets": 25,
  "concurrency": 25,
  "elapsedSeconds": 1.853,
  "throughputRps": 269.9,
  "latencyMs": {
    "p50": 83.46,
    "p95": 181.86,
    "p99": 306.08
  },
  "errorRate5xx": 0,
  "lockConflicts": 0,
  "outboxLagSeconds": 1.467,
  "allWalletsConsistent": true
}
```

| Métrica | Valor Observado |
|---|---|
| Total de Transações | `500` (além das `25` transações `OPENING`) |
| Tempo Total | `1.853 s` |
| **Throughput** | **`269.90 req/s` (RPS)** |
| **Latência p50** | `83.46 ms` |
| **Latência p95** | `181.86 ms` |
| **Latência p99** | `306.08 ms` |
| **Taxa de Erro (5xx)** | `0.00%` (`0` falhas) |
| **Conflitos de Lock (`lock_timeout` / deadlocks)** | `0` |
| **Outbox Lag ao final da rajada** | `1.467 s` (drenado pelo `OutboxPublisherWorker` em seguida) |
| **Consistência Final (`wallet.balance == Σ ledger`)** | `100%` (`25/25` wallets consistentes) |

## 4. Análise Técnica

1. **Zero Deadlocks e Zero Lock Timeouts**: como todas as operações adquirem o lock exclusivo na linha da wallet (`SELECT ... FOR UPDATE`) sempre na mesma ordem (`wallets` -> `wager_transactions`) e não realizam I/O externo (como chamadas HTTP/SQS) dentro da transação SQL, o tempo de retenção do lock por transação é de poucos milissegundos, permitindo enfileiramento ordenado no PostgreSQL sem estourar o `lock_timeout` de `5s`.
2. **Desacoplamento de Latência via Outbox**: a latência HTTP (p50 de `83.46 ms` sob 25 clientes simultâneos) depende exclusivamente do commit local no PostgreSQL. A publicação em rede para o SQS FIFO ocorre de forma assíncrona via `OutboxPublisherWorker`, mantendo um lag máximo de ~1.47s durante o pico de 270 transações/s (~540 eventos/s gerados na outbox).

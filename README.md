# Jungle Wagering Processor 🦧

Processador financeiro distribuído de transações de apostas (*wagering*) para múltiplos provedores de jogos, construído com **Bun 1.x**, **TypeScript (strict)**, **NestJS 11**, **MikroORM 6**, **PostgreSQL 17** e **AWS SQS FIFO (LocalStack)**.

> Para detalhes profundos de design, invariantes de banco, concorrência, idempotência e trade-offs, consulte o [**ARCHITECTURE.md**](./ARCHITECTURE.md) e o [**Relatório de Teste de Carga**](./docs/LOAD_TEST.md).

---

## 0. Resumo Executivo Didático

Imagine que este sistema é o **caixa central de um cassino online**. Os jogos externos (*provedores*) mandam pedidos do tipo *"o jogador apostou R$ 25,00"* (`BET`), *"ganhou R$ 80,00"* (`WIN`), *"perdeu"* (`LOSS`), *"devolve a aposta"* (`REFUND`) ou *"cancela a operação"* (`ROLLBACK`). Como a rede pode falhar, repetir mensagens ou mandar dois pedidos no mesmo milissegundo, o sistema usa cinco garantias simples:

1. **Carteira (`wallets`)**: cada jogador tem no máximo 1 carteira por moeda (`1 playerId + currency`). O saldo nunca fica negativo e nunca usa `number`/float — apenas decimal exato (`decimal.js` + `NUMERIC(20,2)`).
2. **Caderno à caneta (`wallet_ledger_entries`)**: todo crédito ou débito gera uma linha imutável de extrato (`balance_before` → `amount` → `balance_after`). No banco, um *trigger* impede qualquer `UPDATE` ou `DELETE`.
3. **Etiqueta única (`Idempotency-Key` + `inbox_messages`)**: se o provedor mandar a mesma requisição 50 vezes, só a 1ª altera o saldo; as outras 49 devolvem a mesma resposta original sem cobrar duas vezes.
4. **Cadeado por carteira (`SELECT ... FOR UPDATE`)**: enquanto uma transação mexe na carteira do João, outra transação para a **mesma** carteira espera na fila do banco (~1–5ms). A carteira da Maria continua livre em paralelo.
5. **Gaveta de avisos (`outbox_messages`)**: os eventos de saída para outros sistemas não são disparados no meio da operação. Eles são gravados numa tabela dentro da **mesma transação SQL** do dinheiro e só publicados na fila depois do `COMMIT`.

### Dois canais de entrada, um único cérebro

| Canal | Como chega | Camada de entrada | Cérebro compartilhado | Resposta / Confirmação |
|---|---|---|---|---|
| **HTTP (síncrono)** | `POST /wagering/transactions` com header `Idempotency-Key` | Controllers NestJS + Zod DTOs + `ErrorMapperFilter` | **`ProcessWagerTransactionUseCase`** | Devolve `201` (novo), `200` (replay), `202` (aguardando referência), `422` (regra de negócio), `409` (conflito) ou `503` (erro transitório) |
| **SQS FIFO (assíncrono)** | Mensagem JSON na fila `wager-transactions.fifo` | `WagerTransactionsConsumer` | **`ProcessWagerTransactionUseCase`** | Só apaga da fila (`DeleteMessage`) **depois** do `COMMIT` no PostgreSQL |

### As 3 Filas SQS FIFO (o que entra, o que vai para quarentena e o que sai)

| Fila SQS | Papel | Quem envia | Quem consome / Por que existe |
|---|---|---|---|
| **`wager-transactions.fifo`** | **ENTRADA de pedidos de aposta** | Provedores de jogo externos | `WagerTransactionsConsumer` lê, valida e executa a transação no banco. Usa `MessageGroupId = walletId` como otimização de ordem, mas a correção financeira vem do lock no PostgreSQL. |
| **`wager-transactions-dlq.fifo`** | **QUARENTENA / ERRO (Dead-Letter Queue)** | SQS (após `5` falhas seguidas) ou `WagerTransactionsConsumer` (erro permanente imediato) | Recebe mensagens inválidas/corrompidas ou que falharam demais para **não travar a fila principal**. Fica parada para inspeção operacional (monitorada por `DlqMetricsWorker`). |
| **`wallet-events.fifo`** | **SAÍDA de eventos / recibos** | `OutboxPublisherWorker` (após `COMMIT` no PostgreSQL) | Outros sistemas do cassino (antifraude, bônus, painel ao vivo) leem daqui os eventos `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference` e `WalletBalanceChanged`. |

### Papel de cada camada do projeto

| Pasta / Arquivo | Papel em bom português |
|---|---|
| [`src/domain/`](./src/domain) | **Regras puras de negócio** (`Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry`, `InboxMessage`, `OutboxMessage`, `IntegrationEvent`). Zero dependência de NestJS, banco ou SQS. |
| [`src/application/`](./src/application) | **Orquestração e contratos**: `ProcessWagerTransactionUseCase` (fluxo único), `CreateWalletUseCase`, `ReconcileWalletUseCase`, `PublishOutboxUseCase`, `ResolvePendingReferencesUseCase`, queries, `payload-hash.ts` e interfaces em `ports/`. |
| [`src/infrastructure/`](./src/infrastructure) | **Tecnologia real**: MikroORM 6 + PostgreSQL (`migrations`, `schemas`, `repositories`, `unit-of-work`), mensageria SQS (`sqs-client`, `wager-transactions.consumer`, `sqs-event-publisher`), workers em background (`outbox-publisher`, `pending-reference`, `dlq-metrics`) e observabilidade (`pino` + `prom-client`). |
| [`src/interfaces/http/`](./src/interfaces/http) | **Porta HTTP**: controllers (`wallets`, `wagering`, `providers`, `health`, `metrics`), validação Zod, `correlation.middleware.ts`, `auth.guard.ts` e `error-mapper.filter.ts`. |
| [`src/main.ts`](./src/main.ts) & [`src/app.module.ts`](./src/app.module.ts) | **Bootstrap e injeção de dependências**: liga todas as peças e habilita HTTP/workers pelas flags `RUN_*` e graceful shutdown (`SIGTERM`). |
| [`test/`](./test) | **Provas automatizadas**: `unit/` (regras puras), `integration/` (PostgreSQL + LocalStack reais), `concurrency/` (corridas e crashes com 3 processos reais) e `load/` (teste de carga). |

---

## 1. Pré-requisitos

- **Docker** (v24+) e **Docker Compose** (v2+)
- **Bun 1.x** (ex.: `mise use -g bun@1`)

Portas utilizadas no host (configuradas para não colidir com serviços locais comuns):
- `5433`: PostgreSQL 17 (`wagering` e `wagering_test`)
- `4566`: LocalStack 3.8 (SQS FIFO)
- `3001`, `3002`, `3003`: 3 instâncias concorrentes da aplicação (`app-1`, `app-2`, `app-3`)

---

## 2. Subindo o Cluster Completo (Docker Compose)

Para construir a imagem e subir o PostgreSQL, LocalStack, job de migration (`migrate`) e as **3 instâncias da aplicação** (`app-1`, `app-2`, `app-3`):

```bash
docker compose up --build -d
```

Verificar a saúde das 3 instâncias:

```bash
curl -s http://localhost:3001/health/ready | jq
curl -s http://localhost:3002/health/ready | jq
curl -s http://localhost:3003/health/ready | jq
```

Para derrubar os containers e volumes:

```bash
docker compose down -v
```

---

## 3. Desenvolvimento Local e Comandos

```bash
# Instalar dependências
bun install

# Subir apenas a infraestrutura (Postgres 5433 + LocalStack 4566)
bun run infra:up

# Executar migrations no banco configurado (.env)
bun run migrate:up

# Reverter migrations (down até 0)
bun run migrate:down

# Reaplicar do zero (down -> up)
bun run migrate:fresh

# Checagem estática de tipos (TypeScript strict)
bun run typecheck

# Iniciar aplicação localmente (porta 3001 por padrão)
bun run start
```

### Executando os Testes

Todos os testes de integração e concorrência utilizam **PostgreSQL e LocalStack SQS reais** em containers (banco isolado `wagering_test` e filas SQS dedicadas por execução):

```bash
# Testes de unidade (Money, Wallet, WagerTransaction, Ledger, PayloadHash, regras de negócio)
bun run test:unit

# Testes de integração (migrations up/down/up, constraints/triggers SQL, atomicidade UoW, HTTP API, Outbox, Pending-Reference, SQS Consumer/Inbox/DLQ, Observabilidade)
bun run test:integration

# Testes de concorrência real e injeção de falhas (cenários 1 a 8, incluindo 3 processos OS simultâneos, crash pós-commit/pré-ack, crash pós-lease e SIGKILL)
bun run test:concurrency

# Suíte completa (unit + integration + concurrency)
bun test

# Teste de carga (diferencial opcional — gera métricas reais de throughput, p50/p95/p99 e outbox lag)
bun run test:load
```

---

## 4. Exemplos de Uso da API HTTP (`curl`)

### 4.1 Criar Wallet (`POST /wallets`)

Quando `initialBalance > 0`, gera atomicamente uma transação interna `OPENING` (`PROCESSED`), um lançamento `CREDIT` no ledger (`wallet_version = 1`) e um evento `WalletBalanceChanged` na outbox:

```bash
curl -i -X POST http://localhost:3001/wallets \
  -H "Content-Type: application/json" \
  -H "X-Correlation-Id: demo-create-wallet" \
  -d '{
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "initialBalance": { "amount": "1000.00", "currency": "BRL" }
  }'
```

### 4.2 Consultar Wallet (`GET /wallets/:walletId`)

```bash
curl -s http://localhost:3001/wallets/0192f291-27dd-7d3f-8071-5f8685deef37 | jq
```

### 4.3 Submeter Aposta — `BET` (`POST /wagering/transactions`)

O header `Idempotency-Key` é obrigatório:

```bash
curl -i -X POST http://localhost:3001/wagering/transactions \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: provider-a:transaction-123" \
  -d '{
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }'
```

- **1ª chamada**: `201 Created` (`"idempotentReplay": false`, `"balance": { "amount": "975.00", "currency": "BRL" }`)
- **Repetição idêntica**: `200 OK` (`"idempotentReplay": true`, retorna o mesmo saldo observado no momento da execução original)
- **Mesma `Idempotency-Key` com payload diferente**: `409 Conflict` (`IDEMPOTENCY_CONFLICT`)
- **Saldo insuficiente / violação de regra de negócio**: `422 Unprocessable Entity` (`"status": "REJECTED"`, `"failureCode": "INSUFFICIENT_FUNDS"`)
- **Referência ainda ausente (`REFUND`/`ROLLBACK` fora de ordem)**: `202 Accepted` (`"status": "PENDING_REFERENCE"`)

### 4.4 Consultar Transação por ID ou por ID Externo do Provedor

```bash
# Por ID interno
curl -s http://localhost:3001/wagering/transactions/0192f298-345e-7e38-af88-e43f851a819d | jq

# Por (providerId, externalTransactionId)
curl -s http://localhost:3001/providers/provider-a/wagering/transactions/transaction-123 | jq
```

### 4.5 Consultar Ledger Paginado (`GET /wallets/:walletId/ledger`)

```bash
curl -s "http://localhost:3001/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/ledger?limit=50" | jq
```

Se houver próxima página, passe `nextCursor` em `?cursor=<nextCursor>&limit=50`.

### 4.6 Reconciliação de Wallet (`POST /wallets/:walletId/reconciliation`)

Compara o saldo materializado na tabela `wallets` com a soma algébrica (`Σ CREDIT - Σ DEBIT`), última versão e último `balance_after` do `wallet_ledger_entries`:

```bash
curl -s -X POST http://localhost:3001/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/reconciliation | jq
```

### 4.7 Health Checks e Métricas Prometheus

```bash
curl -s http://localhost:3001/health/live | jq
curl -s http://localhost:3001/health/ready | jq
curl -s http://localhost:3001/metrics
```

---

## 5. Enviando Mensagens via AWS SQS (LocalStack)

As filas FIFO inicializadas automaticamente no LocalStack (`http://localhost:4566`) são:
- `wager-transactions.fifo` (entrada de comandos)
- `wager-transactions-dlq.fifo` (Dead-Letter Queue)
- `wallet-events.fifo` (destino dos eventos de integração publicados pela Transactional Outbox)

Exemplo enviando uma operação `WIN` pela fila `wager-transactions.fifo`:

```bash
aws --endpoint-url=http://localhost:4566 --region us-east-1 sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "0192f291-27dd-7d3f-8071-5f8685deef37" \
  --message-deduplication-id "msg-win-001" \
  --message-body '{
    "messageId": "msg-win-001",
    "type": "WagerTransactionRequested",
    "occurredAt": "2026-07-29T15:00:00.000Z",
    "data": {
      "providerId": "provider-a",
      "externalTransactionId": "transaction-124",
      "idempotencyKey": "provider-a:transaction-124",
      "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
      "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
      "roundId": "round-987",
      "gameId": "fortune-chimp",
      "kind": "WIN",
      "money": { "amount": "80.00", "currency": "BRL" },
      "referenceExternalTransactionId": "transaction-123"
    }
  }'
```

Lendo eventos publicados pela Outbox em `wallet-events.fifo`:

```bash
aws --endpoint-url=http://localhost:4566 --region us-east-1 sqs receive-message \
  --queue-url http://localhost:4566/000000000000/wallet-events.fifo \
  --max-number-of-messages 10
```

---

## 6. Variáveis de Ambiente (`.env.example`)

| Variável | Default | Descrição |
|---|---|---|
| `PORT` | `3001` | Porta HTTP do processo |
| `INSTANCE_ID` | `app-local-1` | Identificador único da instância para logs e lease da outbox |
| `LOG_LEVEL` | `info` | Nível do logger estruturado JSON (`pino`) |
| `RUN_HTTP` | `true` | Habilita servidor HTTP |
| `RUN_CONSUMER` | `true` | Habilita consumidor SQS (`wager-transactions.fifo`) |
| `RUN_OUTBOX` | `true` | Habilita worker publisher da Transactional Outbox |
| `RUN_PENDING` | `true` | Habilita worker de resolução de `PENDING_REFERENCE` |
| `RUN_DLQ_METRICS` | `true` | Habilita coletor periódico de profundidade da DLQ |
| `DB_HOST` / `DB_PORT` | `localhost` / `5433` | Conexão PostgreSQL |
| `DB_USER` / `DB_PASSWORD` / `DB_NAME` | `wagering` / `wagering_secret` / `wagering` | Credenciais e database |
| `DB_LOCK_TIMEOUT_MS` | `5000` | `SET LOCAL lock_timeout` por transação SQL |
| `DB_STATEMENT_TIMEOUT_MS` | `10000` | `SET LOCAL statement_timeout` por transação SQL |
| `SQS_ENDPOINT` | `http://localhost:4566` | Endpoint do LocalStack / SQS |
| `SQS_VISIBILITY_TIMEOUT_SEC` | `30` | VisibilityTimeout padrão da fila principal |
| `SQS_MAX_RECEIVE_COUNT` | `5` | Tentativas máximas antes de envio para a DLQ |
| `OUTBOX_POLL_INTERVAL_MS` / `OUTBOX_LEASE_SEC` | `500` / `30` | Intervalo e tempo de lease do publisher da outbox |
| `PENDING_REF_MAX_ATTEMPTS` / `PENDING_REF_TTL_MS` | `10` / `900000` (15m) | Limite de tentativas e TTL para `PENDING_REFERENCE` |
| `SHUTDOWN_GRACE_MS` | `20000` | Tempo máximo de espera por mensagens in-flight no `SIGTERM` |

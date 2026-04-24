# BBS — Bulletin Board System

Sistema de troca de mensagens instantâneas distribuído, inspirado em BBS e IRC, desenvolvido para a disciplina de Sistemas Distribuídos.

## Integrantes e Linguagens

| Integrante | Linguagem | Servidor | Cliente |
|---|---|---|---|
| 1 | Python | server-python | client-python |
| 2 | Go | server-go | client-go |
| 3 | C# | server-csharp | client-csharp |
| 4 | C | server-c | client-c |
| 5 | Lua | server-lua | client-lua |

## Como Executar

```bash
docker compose down -v
docker compose up --build
```

Sobem: 1 reference + 1 proxy + 5 servidores + 10 clientes (2 por linguagem) = **17 containers**.

---

## Arquitetura Geral (Partes 1, 2 e 3)

```
                 ┌──────────────────────┐
                 │   REFERENCE :5559    │   (Parte 3)
                 │  register/list/hb    │
                 └─────────▲────────────┘
                           │ REQ/REP
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   server-python     server-go         server-c/cs/lua
   (REP :5550)       (REP :5551)       (REP :5552/5553/5554)
        │                  │                  │
        │ PUB              │ PUB              │ PUB
        └──────────────────┼──────────────────┘
                           ▼
                 ┌──────────────────────┐
                 │   PROXY              │
                 │   XSUB :5557         │
                 │   XPUB :5558         │
                 └──────────▲───────────┘
                            │ SUB
        ┌───────────────────┼───────────────────┐
        │                   │                   │
  client-python-1,2   client-go-1,2       client-c/cs/lua-1,2
  (REQ→server)        (REQ→server)        (REQ→server)
  (SUB←proxy)         (SUB←proxy)         (SUB←proxy)
```

Cada **cliente** possui dois sockets:
- `REQ` → servidor da sua linguagem (login, canal, publish)
- `SUB` → proxy XPUB (recebe mensagens dos canais inscritos)

Cada **servidor** possui três sockets:
- `REP` → atende clientes
- `PUB` → envia publicações para o proxy XSUB
- `REQ` → comunica com o reference (register + heartbeat)

---

## Parte 1 — Login e Canais

### Mensagens implementadas
```
Cliente → Servidor: { type: "login",   username, timestamp, clock }
Cliente → Servidor: { type: "channel", username, channel_name, timestamp, clock }
Cliente → Servidor: { type: "list",    username, timestamp, clock }
Servidor → Cliente: { status, message, data?, timestamp, clock, rank? }
```

### Tratamento de erros
- **Login**: username vazio → erro. Username válido sempre aceito (sem senha, conforme enunciado).
- **Canal duplicado**: retorna `status=error` com mensagem; o bot pode continuar operando.
- **Nome de canal inválido**: aceita apenas alfanuméricos, `-` e `_`, com até 32 caracteres.

---

## Parte 2 — Publicação Pub/Sub

### Fluxo de publicação
```
Cliente  → Servidor: REQ  { type: "publish", channel_name, message, timestamp, clock }
Servidor → Proxy:    PUB  [topic=channel_name][msgpack payload]
Proxy    → Clientes: broadcast p/ quem inscreveu no tópico
Servidor → Cliente:  REP  { status: "ok"|"error", ... }
```

### Inscrição em canais
- Escolhida **apenas no cliente** (proxy recebe `zmq.SUBSCRIBE` via socket SUB).
- Cada bot se inscreve em até **3 canais aleatórios** da lista.

### Comportamento padronizado dos bots
1. Login no servidor.
2. Se existirem menos de 5 canais → cria um novo.
3. Inscreve-se em até 3 canais aleatórios.
4. **Loop infinito**: escolhe um canal aleatório → publica 10 mensagens com 1s de intervalo → atualiza lista de canais → repete.

### Proxy
- Container Python usando `zmq.proxy(XSUB, XPUB)`.
- `5557` = XSUB (servidores publicam aqui).
- `5558` = XPUB (clientes se inscrevem aqui).

---

## Parte 3 — Relógios e Heartbeat

### Relógio lógico (Lamport) — em TODOS os processos
Implementado em clientes, servidores e no próprio reference service:

```
tick_send():  clock += 1;                        → envia clock
tick_recv(r): clock = max(clock, r);             → ao receber
```

Toda mensagem trocada carrega um campo `clock`. A cada envio, o contador é incrementado; a cada recebimento, aplica-se `max(local, recebido)`.

### Serviço de Referência (`reference`)
Container novo em Python, escutando em `tcp://*:5559` (REP), responsável por:

1. **Register** — servidor manda `{type:"register", name}`, recebe `{rank}` atribuído por ordem de chegada, sem repetir nomes.
2. **List** — retorna `[{name, rank}, ...]` de todos servidores ativos.
3. **Heartbeat** — servidor envia a cada 10 mensagens de cliente recebidas. O reference:
   - atualiza `last_beat` daquele servidor,
   - responde com `{time: <tempo_correto>, clock, ...}` para sincronização do relógio físico.
4. **Watchdog** — thread em background que roda a cada 10s. Se um servidor não enviou heartbeat nos últimos **30 segundos**, é removido da lista.

### Sincronização do relógio físico
Cada servidor mantém `time_offset = ref_time − local_time`. Toda vez que chega um ACK de heartbeat com o campo `time`, o offset é recalculado. O `now_ts()` do servidor sempre aplica esse offset, então timestamps persistidos e publicados ficam alinhados entre todos os servidores.

### Fluxo do heartbeat
```
Servidor → Reference: REQ { type:"heartbeat", name, clock, timestamp, msg_count }
Reference → Servidor: REP { status:"ok", time, clock, timestamp }
Servidor: time_offset = time − local_time()
```

---

## Escolhas Técnicas

### Serialização: **MessagePack**
Formato binário compacto com suporte nas 5 linguagens (`msgpack`, `MessagePack-CSharp`, `vmihailenco/msgpack/v5`, `mpack` para C, `lua-messagepack`). Menor que JSON, mais rápido que XML.

### Persistência: **SQLite**
Cada servidor tem seu próprio `/data/server.db` (volume Docker nomeado `data-<lang>`). Tabelas:

| Tabela | Campos |
|---|---|
| `users` | username, created_at |
| `logins` | id, username, timestamp |
| `channels` | name, created_by, created_at |
| `messages` | id, channel, username, message, timestamp, **clock** |

A coluna `clock` foi adicionada na Parte 3 — cada mensagem publicada é salva junto com o valor do relógio lógico do cliente que a enviou.

### Lua — timestamp com precisão de milissegundos
Usa `socket.gettime()` (da biblioteca `luasocket`) em vez de `os.time()`, para que o `time_offset` recebido do reference seja aplicado corretamente.

---

## Portas

| Serviço | Porta |
|---|---|
| reference (REP) | 5559 |
| proxy XSUB | 5557 |
| proxy XPUB | 5558 |
| server-python | 5550 |
| server-go | 5551 |
| server-csharp | 5552 |
| server-c | 5553 |
| server-lua | 5554 |

## Variáveis de ambiente (docker-compose)

| Variável | Onde | Descrição |
|---|---|---|
| `PORT` | servidores | Porta do REP do servidor |
| `PROXY_HOST` | servidores/clientes | Hostname do proxy |
| `XSUB_PORT` / `XPUB_PORT` | servidores/clientes | Portas do proxy |
| `REF_HOST` / `REF_PORT` | servidores | Endereço do reference |
| `SERVER_NAME` | servidores | Nome único usado pelo reference |
| `BOT_NAME` | clientes | Nome do bot (enviado no login) |
| `SERVER_HOST` / `SERVER_PORT` | clientes | Servidor a qual se conectam |
| `HEARTBEAT_TIMEOUT` | reference | Segundos sem heartbeat → remoção (default **30s**) |

---

## Formato final das mensagens (Parte 3)

**Requisição cliente → servidor:**
```json
{ "type": "...", "username": "...", "channel_name": "...", "message": "...",
  "timestamp": 1730000000.123, "clock": 42 }
```

**Resposta servidor → cliente:**
```json
{ "status": "ok|error", "message": "...", "data": [...],
  "timestamp": 1730000000.124, "clock": 43, "rank": 2 }
```

**Publicação servidor → proxy → clientes SUB:**
```
Frame 1: channel_name (texto, usado como tópico)
Frame 2: msgpack { channel, username, message, timestamp, received, clock }
```

**Reference server ↔ servidor:**
```json
REQ: { "type": "register|list|heartbeat", "name": "server-X",
       "clock": N, "timestamp": T, "msg_count": M }
REP: { "status": "ok", "rank": R, "time": T_ref,
       "servers": [{"name","rank"}], "clock": N, "timestamp": T }
```

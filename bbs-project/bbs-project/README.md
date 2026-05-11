# BBS - Bulletin Board System — Versão Final (Parte 5)

## Como Executar

```bash
docker compose down -v
docker compose up --build
```

---

## Arquitetura Geral

```
Clientes (bots) ──REQ/REP──▶ Servidores ──PUB──▶ Proxy ──SUB──▶ Clientes
                                  │                  │
                                  └─────────SUB◀─────┘ (replicação)
                                  │
                              Referência (rank, heartbeat, lista)
                                  │
                    Eleição S2S ──┘ Coordenador (sync de relógio)
```

---

## Parte 5: Replicação — Método Escolhido

### Método: Replicação Passiva via PUB/SUB

**Justificativa da escolha:**

O projeto já possui uma infraestrutura de PUB/SUB (proxy XSUB/XPUB). Aproveitamos essa infraestrutura para implementar replicação passiva sem adicionar nenhum novo container ou protocolo.

**Como funciona:**

1. Quando um servidor recebe uma mensagem de publicação de um cliente, ele:
   - Armazena a mensagem localmente no SQLite com um `msg_id` único
   - Publica a mensagem no proxy via socket PUB, incluindo o campo `origin` (nome do servidor que originou)

2. Cada servidor possui uma **thread de replicação** que:
   - Conecta ao proxy via socket SUB com assinatura vazia (`""`) — recebe TODOS os tópicos
   - Ignora o tópico `servers` (usado para eleição de coordenador)
   - Para cada mensagem recebida: tenta inserir com `INSERT OR IGNORE` usando o `msg_id` como chave única

3. A **deduplicação** é garantida por:
   - Campo `msg_id TEXT UNIQUE` na tabela `messages`
   - O `msg_id` é um hash dos campos (channel, username, message, timestamp)
   - O servidor que originou a mensagem não a armazena duas vezes pois o INSERT OR IGNORE ignora duplicatas

**Mudanças em relação ao método original:**

O método de replicação passiva classicamente usa um servidor primário que propaga para réplicas passivas. Aqui adaptamos para funcionar com múltiplos servidores iguais (sem hierarquia de primário/réplica) usando o barramento PUB/SUB já existente. Todos os servidores são primários para seus próprios clientes e réplicas para mensagens dos outros servidores.

**Diagrama do fluxo de replicação:**

```
bot-py-1 ──publish──▶ server-python ──PUB(canal,msg,origin=server-python)──▶ PROXY
                                                                                 │
                           server-go ◀──SUB──────────────────────────────────────┤
                       server-csharp ◀──SUB──────────────────────────────────────┤
                           server-c  ◀──SUB──────────────────────────────────────┤
                         server-lua  ◀──SUB──────────────────────────────────────┘
                        (todos salvam com INSERT OR IGNORE)
```

**O que aparece no terminal:**

```
server-python  | PUB  | channel=geral | from=bot-py-1 | clock=42
server-go      | REPL | channel=geral | from=bot-py-1 | origin=server-python
server-csharp  | REPL | channel=geral | from=bot-py-1 | origin=server-python
server-c       | REPL | channel=geral | from=bot-py-1 | origin=server-python
server-lua     | REPL | channel=geral | from=bot-py-1 | origin=server-python
```

---

## Relógio Lógico (Lamport)

Implementado em clientes e servidores. Regras:
- Incrementado antes de cada envio (`clock++`)
- Ao receber: `clock = max(clock_local, clock_recebido)`
- Presente em todas as mensagens (REQ/REP e PUB/SUB)

**O que aparece:**
```
server-python | RECV | type=publish | from=bot-py-1 | clock=21 | lc=25
server-python | SEND | status=ok   | clock=26
client-py-1   | MSG  | channel=... | clock=27 | sent=... | recv=...
```

---

## Sincronização de Relógio — Berkeley Simplificado

- Cada servidor contacta o coordenador a cada 15 mensagens
- Coordenador responde com sua hora atual
- Servidor calcula e aplica o `time_offset`

**O que aparece:**
```
server-go | CLOCK SYNC | coord=server-c | ref_time=1234567890.123 | offset=-0.000200
```

---

## Eleição de Coordenador (Bully Simplificado)

- Servidor com **menor rank** vence
- Ao iniciar ou quando coordenador não responde: envia REQ `election` para todos via S2S
- Vencedor publica no tópico `servers` via proxy

**O que aparece:**
```
server-c  | ELECTED as coordinator | clock=34
server-go | New coordinator: 'server-c'
```

---

## Heartbeat

- Enviado à referência a cada 15 mensagens
- Referência remove servidor após 30s sem heartbeat

**O que aparece:**
```
reference     | HEARTBEAT from 'server-go' | clock=93
reference     | REMOVE server 'server-lua' (no heartbeat)
```

---

## Portas

| Serviço | Porta cliente | Porta S2S |
|---|---|---|
| Proxy XSUB | 5557 | — |
| Proxy XPUB | 5558 | — |
| Reference  | 5559 | — |
| server-python | 5550 | 5560 |
| server-go     | 5551 | 5561 |
| server-csharp | 5552 | 5562 |
| server-c      | 5553 | 5563 |
| server-lua    | 5554 | 5564 |

---

## Linguagens

| Integrante | Linguagem |
|---|---|
| 1 | Python |
| 2 | Go |
| 3 | C# |
| 4 | C |
| 5 | Lua |
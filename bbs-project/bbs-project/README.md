# BBS - Bulletin Board System

Sistema de troca de mensagens instantâneas distribuído, inspirado em BBS e IRC.

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

## Arquitetura

```
                    ┌─────────────────────────────┐
                    │         PROXY               │
                    │   XSUB :5557  XPUB :5558    │
                    └──────────┬──────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
   server-python          server-go           server-c ...
   (PUB→5557)             (PUB→5557)          (PUB→5557)
          │
   client-python-1 (SUB←5558, REQ→server)
   client-python-2 (SUB←5558, REQ→server)
```

Cada cliente possui **dois sockets**:
- `REQ` → servidor da sua linguagem (login, canal, publish)
- `SUB` → proxy XPUB (recebe mensagens dos canais inscritos)

Cada servidor possui **dois sockets**:
- `REP` → responde clientes
- `PUB` → envia para o proxy XSUB

## Escolhas Técnicas

### Serialização: MessagePack
Formato binário compacto, rápido, com suporte em todas as 5 linguagens.

### Formato das Mensagens

**Requisição (cliente → servidor):**
```
{ type, username, channel_name?, message?, timestamp }
```

**Resposta (servidor → cliente):**
```
{ status, message, data?, timestamp }
```

**Publicação (servidor → proxy → clientes SUB):**
```
Frame 1: channel_name (tópico)
Frame 2: msgpack{ channel, username, message, timestamp, received }
```

### Persistência: SQLite
Cada servidor mantém seu próprio `/data/server.db` com tabelas:
- `users` — usuários registrados
- `logins` — histórico de logins com timestamp
- `channels` — canais criados
- `messages` — **todas as mensagens publicadas** (canal, autor, texto, timestamp)

### Comportamento dos Bots (Parte 2)
1. Login no servidor
2. Se < 5 canais existirem → cria um novo canal
3. Inscreve-se em até 3 canais aleatórios via SUB socket
4. Loop infinito: escolhe canal aleatório → envia 10 mensagens com intervalo de 1s

### Proxy
Container Python simples usando `zmq.proxy(XSUB, XPUB)`.
- Porta `5557` → XSUB (servidores publicam aqui)
- Porta `5558` → XPUB (clientes se inscrevem aqui)

## Portas

| Serviço | Porta |
|---|---|
| Proxy XSUB | 5557 |
| Proxy XPUB | 5558 |
| server-python | 5550 |
| server-go | 5551 |
| server-csharp | 5552 |
| server-c | 5553 |
| server-lua | 5554 |

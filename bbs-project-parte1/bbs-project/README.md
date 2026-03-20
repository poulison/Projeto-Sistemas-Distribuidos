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
docker compose up --build
```

Isso irá subir **5 servidores** e **10 clientes** (2 por linguagem) automaticamente.

## Arquitetura

```
client-py-1  ──REQ/REP──▶  server-python  ──── /data/server.db
client-py-2  ─────────────/
client-go-1  ──REQ/REP──▶  server-go      ──── /data/server.db
client-go-2  ─────────────/
client-cs-1  ──REQ/REP──▶  server-csharp  ──── /data/server.db
client-cs-2  ─────────────/
client-c-1   ──REQ/REP──▶  server-c       ──── /data/server.db
client-c-2   ─────────────/
client-lua-1 ──REQ/REP──▶  server-lua     ──── /data/server.db
client-lua-2 ─────────────/
```

Cada servidor se comunica apenas com seus próprios clientes (Parte 1).
Na Parte 2, os servidores passarão a se comunicar entre si via broker.

## Escolhas Técnicas

### Serialização: MessagePack

Escolhemos **MessagePack** por ser um formato binário compacto, rápido e com suporte nativo em todas as 5 linguagens usadas. É mais eficiente que JSON/XML e tem bibliotecas maduras em Python (`msgpack`), Go (`vmihailenco/msgpack`), C# (`MessagePack-CSharp`), C (`mpack`) e Lua (`lua-messagepack`).

### Formato da Mensagem

**Requisição (cliente → servidor):**
```
{
  "type":         string  -- "login" | "channel" | "list"
  "username":     string  -- nome do bot
  "channel_name": string  -- apenas para type="channel"
  "timestamp":    float64 -- unix epoch em segundos
}
```

**Resposta (servidor → cliente):**
```
{
  "status":    string   -- "ok" | "error"
  "message":   string   -- descrição do resultado
  "data":      [string] -- apenas para type="list" (lista de canais)
  "timestamp": float64  -- unix epoch em segundos
}
```

### Persistência: SQLite

Cada servidor mantém seu próprio arquivo SQLite em `/data/server.db` (volume Docker separado por linguagem). Tabelas criadas:

- **users**: registro dos usuários que fizeram login
- **logins**: histórico de todos os logins com timestamp
- **channels**: canais criados pelos usuários

SQLite foi escolhido por ser embutido (sem servidor extra), ter suporte em todas as linguagens e ser suficiente para os requisitos do projeto.

### Regras de Negócio

- **Login**: qualquer nome de usuário não-vazio é aceito. Usuários já existentes podem logar novamente (login é registrado no histórico).
- **Criação de canal**: nome deve ter 1–32 caracteres alfanuméricos. Canais duplicados retornam erro (mas não interrompem o bot).
- **Listagem de canais**: retorna todos os canais em ordem de criação.

## Portas

| Linguagem | Porta |
|---|---|
| Python | 5550 |
| Go | 5551 |
| C# | 5552 |
| C | 5553 |
| Lua | 5554 |

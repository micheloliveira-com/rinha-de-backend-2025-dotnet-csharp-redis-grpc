# Rinha de Backend 2025 - API em .NET 9 com AOT

API de pagamentos desenvolvida para o desafio [Rinha de Backend 2025](https://github.com/zanfranceschi/rinha-de-backend-2025), com foco em performance extrema, inicialização instantânea e concorrência eficiente. A aplicação é construída com .NET 9 em modo AOT (Ahead-of-Time), garantindo uso mínimo de recursos e latência reduzida.

PS: Nem todas as implementações feitas neste projeto são recomendadas para cenários reais de produção e algumas delas foram desenvolvidas especificamente para o desafio Rinha de Backend 2025.

## Stack

- **.NET 9 (AOT)** - Gerando um executável nativo
- **ReactiveLock** - Lock distribuído e reativo via Redis para maximinizar a consistência entre as instâncias
- **PostgreSQL e Redis** - Persistência e enfileiramento de mensagens
- **Dapper + Dapper.AOT** - ORM leve e compatível com AOT
- **Polly** - Política de retry resiliente para conexões externas
- **Nginx** - Proxy reverso para balanceamento de carga entre as duas instâncias

```mermaid

graph TD
  loadBalancer["<b>Load Balancer</b><br />(NGINX 1.29.0-alpine)"]

  subgraph backendsGroup["<b>BACKENDS</b>"]
    backend1["<b>Backend-1</b><br />(.NET 9.0-alpine)"]
    backend2["<b>Backend-2</b><br />(.NET 9.0-alpine)"]
    reactiveLock["<b>Lock Distribuído</b><br/>(lib <b>ReactiveLock</b> para sincronia entre processos<br/>HTTP, PostGres, API de Sumário)"]
  end

  subgraph storageGroup["<b>ARMAZENAMENTO E MENSAGERIA</b>"]
    postgres["<b>PostgreSQL</b><br />(postgres:17-alpine)"]
    spacer[" "]:::invisible
    redis["<b>Redis</b><br />(redis:8-alpine)"]
  end

  loadBalancer --> backendsGroup

  backend1 --> storageGroup
  backend2 --> storageGroup

  backend1 --> reactiveLock
  backend2 --> reactiveLock

  reactiveLock --> storageGroup

  classDef invisible fill:none,stroke:none;

  %% Estilos de cor
  style loadBalancer fill:#256D85,stroke:#1B4B57,stroke-width:2px,color:#FFFFFF

  style backendsGroup fill:#a8c7ff,stroke:#333,stroke-width:2px,color:#000
  style backend1 fill:#c9ddff,stroke:#333,stroke-width:1px,color:#000
  style backend2 fill:#c9ddff,stroke:#333,stroke-width:1px,color:#000
  style reactiveLock fill:#f2c14e,stroke:#b8860b,stroke-width:2px,color:#000

  style storageGroup fill:#a8d5a2,stroke:#333,stroke-width:2px,color:#000
  style postgres fill:#c6e0b4,stroke:#333,stroke-width:1px,color:#000
  style redis fill:#c6e0b4,stroke:#333,stroke-width:1px,color:#000
  

```

## Endpoints

- `POST /payments` - Enfileira um pagamento para processamento assíncrono
- `GET /payments-summary` - Retorna um resumo agregado dos pagamentos
- `POST /purge-payments` - Remove os registros de pagamento do sistema

---

## Especificações arquiteturais

- Uso da biblioteca `ReactiveLock` garante consistência máxima entre múltiplas instâncias sem perda de integridade. Esse, por sua vez, também reage e controla o estado de processamento das requisições HTTP, PostgreSQL e API de sumário entre as instâncias.
- Utiliza inserção em lote (bulk insert) de 100 registros no `PostgreSQL` para otimizar a performance com consistência sincronizada entre as instâncias.
- O lock coordenado ocorre especialmente quando a chamada ao endpoint de `GET /payments-summary` é realizada, sincronizando o flush dos lotes do postgres para manter a integridade dos dados antes de realizar a query no banco de dados.
- O `Redis` utilizado para enfileirar as mensagens recebidas garante um pool consistente e balanceado de workers, diferente do envio das requisições para uma única instância via round robin, que não assegura balanceamento adequado devido à variabilidade no tempo de execução entre as requisições a api de pagamentos.
- Totalmente compatível com build AOT, sem reflection dinâmica nem expressões incompatíveis.
- Pronto para ambientes de alta concorrência, ideal para benchmarks e cenários de stress.

## Como rodar

### Subir a stack com Docker Compose
Esse comando irá compilar a aplicação em AOT e subir Redis, PostgreSQL e NGINX para uso local.
```bash
cd src
docker compose build --no-cache
docker compose up -d
```

## Licença

MIT © Michel Oliveira

Para sugestões, dúvidas ou contribuições, fique à vontade para abrir uma issue ou pull request.

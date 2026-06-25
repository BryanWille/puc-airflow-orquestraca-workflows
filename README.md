# Atividade 01 - Apache Airflow

## ShopBrasil - Pipeline de Métricas por Categoria

Este projeto implementa um pipeline de dados em Apache Airflow para substituir um script Python executado via `cron`.

O pipeline coleta produtos da FakeStore API, calcula métricas de preço por categoria e grava os resultados em uma base PostgreSQL.

---

## O que o pipeline faz

A DAG `shopbrasil_pricing_pipeline` executa as seguintes etapas:

1. Busca os produtos na FakeStore API;
2. Valida o schema básico dos produtos;
3. Identifica automaticamente as categorias disponíveis;
4. Calcula métricas por categoria:
   - preço médio;
   - preço mínimo;
   - preço máximo;
   - quantidade de produtos;
5. Grava os dados no PostgreSQL;
6. Evita duplicidade na tabela principal usando escrita idempotente;
7. Mantém uma tabela histórica com as execuções da DAG.

---

## Tecnologias utilizadas

- Apache Airflow
- Python
- Docker
- Docker Compose
- PostgreSQL
- FakeStore API
- TaskFlow API
- Dynamic Task Mapping
- PostgresHook

---

## Principais recursos implementados

- DAG agendada diariamente às 06:00;
- Timezone configurado para `America/Sao_Paulo`;
- `catchup=False`;
- Retry com exponential backoff na task de ingestão;
- Callbacks de sucesso, retry e falha;
- TaskGroups para organizar o pipeline;
- Processamento paralelo por categoria com Dynamic Task Mapping;
- Pool `ecommerce_pool` com 2 slots para limitar concorrência;
- Persistência em PostgreSQL com `PostgresHook`;
- Escrita idempotente para evitar duplicidade;
- Operador customizado para validação dos produtos.

---

## Estrutura do projeto

```text
shopbrasil-airflow/
├── dags/
│   └── shopbrasil_pricing_dag.py
├── plugins/
│   └── validar_produtos_operator.py
├── sql/
│   └── init.sql
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Como executar

Subir os containers:

```bash
docker compose up --build
```

Acessar o Airflow:

```text
http://localhost:5080
```

Credenciais:

```text
admin / admin
```

Criar o pool obrigatório:

```bash
docker compose exec airflow-webserver airflow pools set ecommerce_pool 2 "Pool para limitar processamento paralelo por categoria"
```

Executar a DAG manualmente:

```bash
docker compose exec airflow-webserver airflow dags trigger shopbrasil_pricing_pipeline
```

---

## Validação no PostgreSQL

Consultar a tabela principal:

```bash
docker compose exec postgres psql -U airflow -d shopbrasil -c "SELECT * FROM category_price_snapshot;"
```

Consultar a tabela histórica:

```bash
docker compose exec postgres psql -U airflow -d shopbrasil -c "SELECT * FROM category_price_history;"
```

Validar que não há duplicidade na tabela principal:

```bash
docker compose exec postgres psql -U airflow -d shopbrasil -c "SELECT run_date, category, COUNT(*) FROM category_price_snapshot GROUP BY run_date, category HAVING COUNT(*) > 1;"
```

Resultado esperado:

```text
(0 rows)
```

---

## Resumo

O projeto entrega um pipeline dockerizado em Apache Airflow, com ingestão resiliente, processamento paralelo por categoria, persistência idempotente em PostgreSQL e histórico de execuções.

A solução atende ao cenário da ShopBrasil, tornando o processo mais confiável, escalável e observável do que o script anterior executado via `cron`.

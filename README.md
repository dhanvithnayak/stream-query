<div id="top"></div>

<div align="center">
  <h1>StreamQuery</h1>
  <p>
    An end-to-end streaming ELT pipeline featuring automated data-quality contract gates and an LLM-powered natural language query agent.
  </p>
  <p>
    <a href="#architecture-overview">Architecture</a>
    ·
    <a href="#getting-started">Quickstart</a>
    ·
    <a href="#usage-walkthrough">Walkthrough</a>
    ·
    <a href="#schema-contract-enforcement">Data Contracts</a>
    ·
    <a href="https://github.com/dhanvithnayak/stream-query/issues">Issues</a>
  </p>
</div>

<details>
<summary>Table of Contents</summary>

- [About The Project](#about-the-project)
  - [Architecture Overview](#architecture-overview)
  - [Core Technologies](#core-technologies)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Service Endpoints](#service-endpoints)
- [Usage Walkthrough](#usage-walkthrough)
  - [1. Emit Order Events](#1-emit-order-events)
  - [2. Orchestrate ELT Pipeline](#2-orchestrate-elt-pipeline)
  - [3. Query Marts via Natural Language](#3-query-marts-via-natural-language)
- [Schema Contract Enforcement](#schema-contract-enforcement)
- [Project Structure](#project-structure)
- [License](#license)

</details>

## About The Project

Modern analytical stacks often suffer from two disconnects: real-time streaming data is separated from dimensional data modeling, and business stakeholders lack an accessible way to query warehouse marts without writing complex SQL.

**StreamQuery** addresses this by uniting streaming ingestion, dimensional transformation with strict contract enforcement, and natural language self-serve querying into a single cohesive pipeline:

1. High-throughput e-commerce order streams are captured via Kafka and loaded into a staging warehouse with idempotent micro-batch upserts.
2. Apache Airflow orchestrates dbt transformation workflows that enforce rigorous schema contracts (uniqueness, referential integrity, and value assertions) before promoting data into dimensional marts.
3. An LLM-powered FastAPI service injects live catalog DDL and business rules into prompt context, enabling non-technical users to query data marts using natural language.

### Architecture Overview

```
[Event Producer]
       │
       ▼ (orders.events)
[Apache Kafka (KRaft)]
       │
       ▼ (Micro-Batch Consumer)
[PostgreSQL: raw.orders]
       │
       ▼ (Airflow DAG: streamquery_elt_dag)
[dbt Staging: stg_orders]
       │
       ▼ (Contract Enforcement & Tests)
[dbt Marts: dim_customers, dim_products, fct_orders]
       │
       ▼ (Catalog DDL Context)
[FastAPI NL Query Agent] ──> [Natural Language /query] ──> [Analytical JSON]
```

### Core Technologies

- **Streaming Broker**: Apache Kafka 7.5 running in KRaft mode (no ZooKeeper dependency).
- **Storage & Warehouse**: PostgreSQL 15 with dedicated `raw`, `staging`, and `analytics` schemas (fully compatible with BigQuery dbt profiles).
- **Orchestration**: Apache Airflow 2.8+ running LocalExecutor.
- **Data Modeling & Testing**: dbt-core 1.7+ with staging views, dimensional tables, foreign key relationship tests, and singular SQL assertions.
- **Natural Language Agent**: FastAPI microservice with schema injection, read-only SQL safety guards, multi-provider LLM support (OpenAI, Anthropic, Gemini), and an offline deterministic engine.

<p align="right">(<a href="#top">back to top</a>)</p>

## Getting Started

Follow the steps below to spin up the complete platform locally.

### Prerequisites

- **Docker Engine**: Version 24.0 or higher
- **Docker Compose**: Version 2.20 or higher
- **curl** and **jq** (optional, for CLI query testing)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/dhanvithnayak/stream-query.git
   cd stream-query
   ```

2. Copy the sample environment configuration:
   ```bash
   cp .env.example .env
   ```

3. Launch all services:
   ```bash
   docker compose up -d
   ```

### Service Endpoints

Once the containers are healthy, the following interfaces are accessible:

- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (Default credentials: `airflow` / `airflow`)
- **NL Query Agent Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Agent Health Check**: [http://localhost:8000/health](http://localhost:8000/health)

<p align="right">(<a href="#top">back to top</a>)</p>

## Usage Walkthrough

You can execute the entire end-to-end pipeline using the automated demo script:

```bash
./scripts/run_demo.sh
```

Alternatively, follow the step-by-step manual flow below.

### 1. Emit Order Events

Generate 50 synthetic e-commerce order transactions and stream them into Kafka:

```bash
docker compose exec airflow-webserver python /opt/airflow/producer/produce_events.py \
  --broker kafka:9092 \
  --count 50 \
  --interval 0.05
```

### 2. Orchestrate ELT Pipeline

Trigger the Airflow DAG to ingest Kafka events into `raw.orders`, execute dbt dimensional transformations, and run data-quality contract tests:

```bash
docker compose exec airflow-webserver airflow dags trigger streamquery_elt_dag
```

You can monitor task execution in the Airflow UI, or run the dbt steps directly inside the container:

```bash
docker compose exec airflow-webserver dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
docker compose exec airflow-webserver dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
```

### 3. Query Marts via Natural Language

Submit natural language questions to the query agent endpoint:

#### Example: Sales Performance by Category

```bash
curl -s -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "What are the total sales by product category?"}' | jq
```

*Response:*
```json
{
  "question": "What are the total sales by product category?",
  "generated_sql": "SELECT p.category, COUNT(DISTINCT f.order_id) AS total_orders, SUM(f.quantity) AS total_units_sold, ROUND(SUM(f.gross_amount), 2) AS total_revenue FROM analytics.fct_orders f JOIN analytics.dim_products p ON f.product_id = p.product_id WHERE f.is_completed = 1 GROUP BY p.category ORDER BY total_revenue DESC;",
  "provider": "offline-rule-engine",
  "execution_time_ms": 14.82,
  "row_count": 4,
  "results": [
    {
      "category": "Electronics",
      "total_orders": 18,
      "total_units_sold": 34,
      "total_revenue": 6428.50
    },
    {
      "category": "Home & Kitchen",
      "total_orders": 14,
      "total_units_sold": 22,
      "total_revenue": 3140.20
    }
  ]
}
```

#### Example: Top Customers by Lifetime Value

```bash
curl -s -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Who are our top 5 customers by total spend?"}' | jq
```

<p align="right">(<a href="#top">back to top</a>)</p>

## Schema Contract Enforcement

To prevent corrupt or invalid records from entering downstream analytical marts, StreamQuery enforces strict schema contracts at the ingestion boundary.

You can verify that bad data is trapped and blocked before reaching production tables by running the automated contract failure test:

```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/test_contract_failure.py
```

This test:
1. Injects a test record with an invalid order status (`CORRUPT_INVALID_STATUS`) and a negative unit price into `raw.orders`.
2. Triggers `dbt test`.
3. Asserts that dbt catches the contract violations (`accepted_values` enum test and `assert_positive_order_amounts` singular assertion) and halts the pipeline with an error code.
4. Cleans up the test record to restore the warehouse to a pristine state.

<p align="right">(<a href="#top">back to top</a>)</p>

## Project Structure

```
stream-query/
├── docker-compose.yml              # Kafka, Postgres, Airflow, and Agent API services
├── Dockerfile.airflow              # Airflow image with dbt-postgres and Kafka drivers
├── .env.example                    # Environment variable template
├── requirements.txt                # Root Python dependencies
├── LICENSE                         # MIT License
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── produce_events.py           # Faker synthetic order stream generator
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── load_to_warehouse.py        # Micro-batch Kafka-to-PostgreSQL loader
├── dags/
│   └── streamquery_elt_dag.py      # Airflow ELT orchestration DAG
├── dbt_project/
│   ├── dbt_project.yml             # dbt project definition
│   ├── profiles.yml                # Postgres and BigQuery warehouse profiles
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── staging/
│   │   │   ├── schema.yml          # Source definitions and staging column tests
│   │   │   └── stg_orders.sql      # Cleaned and standardized staging view
│   │   └── marts/
│   │       ├── schema.yml          # Mart schemas, referential integrity & tests
│   │       ├── dim_customers.sql   # Customer dimension (LTV, tiers, order counts)
│   │       ├── dim_products.sql    # Product dimension (revenue, units fulfilled)
│   │       └── fct_orders.sql      # Transactional order fact table
│   └── tests/
│       └── assert_positive_order_amounts.sql
├── agent/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── schema_context.py           # Mart catalog DDL and business rules
│   ├── llm_client.py               # Multi-provider Text-to-SQL engine with safety
│   └── main.py                     # FastAPI service (/query, /health, /schema)
└── scripts/
    ├── init_databases.sh           # PostgreSQL multi-database initialization
    ├── run_demo.sh                 # Turnkey end-to-end demo runner
    └── test_contract_failure.py    # Schema contract verification test
```

<p align="right">(<a href="#top">back to top</a>)</p>

## License

Distributed under the MIT License. See [LICENSE](LICENSE) for more information.

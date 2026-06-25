from datetime import timedelta

import pendulum
import requests

from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from validar_produtos_operator import ValidarProdutosOperator


API_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "ecommerce_postgres"


def alerta_falha(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]

    print(f"[ALERTA] Falha na task {task_id} | DAG: {dag_id} | Run: {run_id}")


def alerta_retry(context):
    task_id = context["task_instance"].task_id
    tentativa = context["task_instance"].try_number

    print(f"[RETRY] Task {task_id} será retentada. Tentativa atual: {tentativa}")


def alerta_sucesso(context):
    task_id = context["task_instance"].task_id

    print(f"[SUCESSO] Task crítica concluída com sucesso: {task_id}")


@dag(
    dag_id="shopbrasil_pricing_pipeline",
    description="Pipeline diário de métricas de preço por categoria para a ShopBrasil",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["shopbrasil", "pricing", "ecommerce"],
)
def shopbrasil_pricing_pipeline():

    @task_group(group_id="ingestao")
    def grupo_ingestao():

        @task(
            task_id="buscar_produtos",
            retries=4,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            on_failure_callback=alerta_falha,
            on_retry_callback=alerta_retry,
            on_success_callback=alerta_sucesso,
        )
        def buscar_produtos():
            try:
                response = requests.get(API_URL, timeout=20)
                response.raise_for_status()

                produtos = response.json()

                if not produtos:
                    raise ValueError("API retornou lista vazia.")

                return produtos

            except Exception as exc:
                print(f"Erro ao buscar produtos da API: {exc}")
                raise

        produtos = buscar_produtos()

        produtos_validados = ValidarProdutosOperator(
            task_id="validar_produtos",
            products=produtos,
        )

        return produtos_validados.output

    @task_group(group_id="analise")
    def grupo_analise(produtos):

        @task(task_id="listar_categorias")
        def listar_categorias(produtos):
            categorias = sorted({produto["category"] for produto in produtos})
            return categorias

        @task(
            task_id="calcular_metricas_categoria",
            pool="ecommerce_pool",
        )
        def calcular_metricas_categoria(category, produtos):
            produtos_categoria = [
                produto for produto in produtos
                if produto["category"] == category
            ]

            precos = [float(produto["price"]) for produto in produtos_categoria]

            return {
                "category": category,
                "avg_price": round(sum(precos) / len(precos), 2),
                "min_price": round(min(precos), 2),
                "max_price": round(max(precos), 2),
                "product_count": len(produtos_categoria),
            }

        categorias = listar_categorias(produtos)

        metricas = calcular_metricas_categoria.partial(
            produtos=produtos
        ).expand(
            category=categorias
        )

        return metricas

    @task_group(group_id="persistencia")
    def grupo_persistencia(metricas):

        @task(task_id="salvar_metricas_postgres")
        def salvar_metricas_postgres(metricas):
            context = get_current_context()

            logical_date = context["logical_date"].in_timezone("America/Sao_Paulo")
            run_date = logical_date.date().isoformat()
            run_id = context["run_id"]

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            create_tables_sql = """
            CREATE TABLE IF NOT EXISTS category_price_snapshot (
                run_date DATE NOT NULL,
                category TEXT NOT NULL,
                avg_price NUMERIC(10, 2) NOT NULL,
                min_price NUMERIC(10, 2) NOT NULL,
                max_price NUMERIC(10, 2) NOT NULL,
                product_count INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_date, category)
            );

            CREATE TABLE IF NOT EXISTS category_price_history (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL,
                run_date DATE NOT NULL,
                category TEXT NOT NULL,
                avg_price NUMERIC(10, 2) NOT NULL,
                min_price NUMERIC(10, 2) NOT NULL,
                max_price NUMERIC(10, 2) NOT NULL,
                product_count INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (run_id, category)
            );
            """

            hook.run(create_tables_sql)

            snapshot_sql = """
            INSERT INTO category_price_snapshot (
                run_date,
                category,
                avg_price,
                min_price,
                max_price,
                product_count,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (run_date, category)
            DO UPDATE SET
                avg_price = EXCLUDED.avg_price,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                product_count = EXCLUDED.product_count,
                updated_at = CURRENT_TIMESTAMP;
            """

            history_sql = """
            INSERT INTO category_price_history (
                run_id,
                run_date,
                category,
                avg_price,
                min_price,
                max_price,
                product_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, category)
            DO UPDATE SET
                avg_price = EXCLUDED.avg_price,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                product_count = EXCLUDED.product_count;
            """

            for metrica in metricas:
                snapshot_params = (
                    run_date,
                    metrica["category"],
                    metrica["avg_price"],
                    metrica["min_price"],
                    metrica["max_price"],
                    metrica["product_count"],
                )

                history_params = (
                    run_id,
                    run_date,
                    metrica["category"],
                    metrica["avg_price"],
                    metrica["min_price"],
                    metrica["max_price"],
                    metrica["product_count"],
                )

                hook.run(snapshot_sql, parameters=snapshot_params)
                hook.run(history_sql, parameters=history_params)

            return {
                "run_date": run_date,
                "categories_saved": len(metricas),
            }

        return salvar_metricas_postgres(metricas)

    produtos = grupo_ingestao()

    metricas = grupo_analise(produtos)

    resultado = grupo_persistencia(metricas)

    produtos >> metricas >> resultado


shopbrasil_pricing_pipeline()

import json
from datetime import timedelta

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "calango_postgres"

ENDPOINTS = {
    "users": "https://fakestoreapi.com/users",
    "products": "https://fakestoreapi.com/products",
    "carts": "https://fakestoreapi.com/carts",
}


@dag(
    dag_id="calango_fakestore_pipeline",
    description="Pipeline distribuído com CeleryExecutor para captura de users, products e carts",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["calango", "celery", "fakestore"],
)
def calango_fakestore_pipeline():

    @task(task_id="criar_tabelas")
    def criar_tabelas():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
        SELECT pg_advisory_xact_lock(2026062601);

        CREATE TABLE IF NOT EXISTS raw_users (
            id INTEGER PRIMARY KEY,
            payload JSONB NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS raw_products (
            id INTEGER PRIMARY KEY,
            payload JSONB NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS raw_carts (
            id INTEGER PRIMARY KEY,
            payload JSONB NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        hook.run(sql)

        return "tables_created"


    @task(
        task_id="capturar_users",
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )
    def capturar_users():
        response = requests.get(ENDPOINTS["users"], timeout=20)
        response.raise_for_status()
        data = response.json()

        return {
            "entity": "users",
            "count": len(data),
            "records": data,
        }

    @task(
        task_id="capturar_products",
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )
    def capturar_products():
        response = requests.get(ENDPOINTS["products"], timeout=20)
        response.raise_for_status()
        data = response.json()

        return {
            "entity": "products",
            "count": len(data),
            "records": data,
        }

    @task(
        task_id="capturar_carts",
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )
    def capturar_carts():
        response = requests.get(ENDPOINTS["carts"], timeout=20)
        response.raise_for_status()
        data = response.json()

        return {
            "entity": "carts",
            "count": len(data),
            "records": data,
        }

    @task(task_id="salvar_users")
    def salvar_users(payload):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
        INSERT INTO raw_users (id, payload, loaded_at)
        VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP)
        ON CONFLICT (id)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            loaded_at = CURRENT_TIMESTAMP;
        """

        for record in payload["records"]:
            hook.run(sql, parameters=(record["id"], json.dumps(record)))

        return {
            "entity": "users",
            "saved": payload["count"],
        }

    @task(task_id="salvar_products")
    def salvar_products(payload):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
        INSERT INTO raw_products (id, payload, loaded_at)
        VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP)
        ON CONFLICT (id)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            loaded_at = CURRENT_TIMESTAMP;
        """

        for record in payload["records"]:
            hook.run(sql, parameters=(record["id"], json.dumps(record)))

        return {
            "entity": "products",
            "saved": payload["count"],
        }

    @task(task_id="salvar_carts")
    def salvar_carts(payload):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
        INSERT INTO raw_carts (id, payload, loaded_at)
        VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP)
        ON CONFLICT (id)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            loaded_at = CURRENT_TIMESTAMP;
        """

        for record in payload["records"]:
            hook.run(sql, parameters=(record["id"], json.dumps(record)))

        return {
            "entity": "carts",
            "saved": payload["count"],
        }

    @task(task_id="consolidar_resultado")
    def consolidar_resultado(users_result, products_result, carts_result):
        return {
            "users": users_result,
            "products": products_result,
            "carts": carts_result,
        }

    tabelas = criar_tabelas()

    users = capturar_users()
    products = capturar_products()
    carts = capturar_carts()

    saved_users = salvar_users(users)
    saved_products = salvar_products(products)
    saved_carts = salvar_carts(carts)

    tabelas >> [users, products, carts]

    resultado = consolidar_resultado(
        saved_users,
        saved_products,
        saved_carts,
    )

    [saved_users, saved_products, saved_carts] >> resultado


calango_fakestore_pipeline()

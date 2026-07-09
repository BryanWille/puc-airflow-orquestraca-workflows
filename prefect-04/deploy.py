from prefect import flow

if __name__ == "__main__":
    # Declaração do arquivo a ser deployado
    flow.from_source(
        source=".",                               # código local, mesma máquina do worker
        entrypoint="example_1.py:meu_pipeline",
    ).deploy(
        name="exemplo-local",
        work_pool_name="aula-04",
        cron="0 20 * * *",        # todo dia às 20:00
    )
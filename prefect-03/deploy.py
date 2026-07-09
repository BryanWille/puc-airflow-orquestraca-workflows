from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source=".",
        entrypoint="example_1.py:meu_pipeline",
    ).deploy(
        name="exemplo-local",
        work_pool_name="aula-04",
        cron="0 20 * * *",
    )

    flow.from_source(
        source=".",
        entrypoint="example_2_block.py:pipeline_clima",
    ).deploy(
        name="exemplo-clima_api_key",
        work_pool_name="aula-04",
        cron="0 22 * * *",
    )

    flow.from_source(
        source=".",
        entrypoint="example_3_block.py:pipeline_auth",
    ).deploy(
        name="exemplo-fake_api_user_pass",
        work_pool_name="aula-04",
        cron="0 21 * * *",
    )

    flow.from_source(
        source=".",
        entrypoint="example_4_map.py:pipeline_precos",
    ).deploy(
        name="exemplo-fake_api_map_fan-out_fan-in",
        work_pool_name="aula-04",
        cron="0 7 * * *",
    )

    flow.from_source(
        source=".",
        entrypoint="example_5_automation_part_1.py:ingestao_produtos",
    ).deploy(
        name="exemplo-automacao-ingestao",
        work_pool_name="aula-04",
        cron="0 7 * * *",
    )

    flow.from_source(
        source=".",
        entrypoint="example_5_automation_part_2.py:gerar_relatorio",
    ).deploy(
        name="exemplo-automacao-relatorio",
        work_pool_name="aula-04",
        cron="0 7 * * *",
    )

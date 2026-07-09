from prefect import flow, task

@task
def fetch_data(url):
    import requests
    return requests.get(url).json()

@task
def process_data(dados):
    return len(dados)

@flow
def meu_pipeline():
    dados = fetch_data("https://fakestoreapi.com/products/1")
    resultado = process_data(dados)
    print(f"Processados: {resultado} registros")

if __name__ == "__main__":
    meu_pipeline()
    # meu_pipeline.serve(
    #     name="meu-deployment",
    #     cron="1 * * * *",
    #     tags=["exemplo_1"]
    # )
import requests
from prefect import flow, task
from prefect.blocks.system import Secret

@task
def buscar_clima(cidade: str) -> dict:
    api_key = Secret.load("api-key-clima").get()    # carrega o segredo pelo NOME
    resp = requests.get(
        "https://api.openweathermap.org/data/2.5/weather",
        params={"q": cidade, "appid": api_key},        # usa a chave
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()

@flow(log_prints=True)
def pipeline_clima():
    dados = buscar_clima("Belo Horizonte")
    print(f"Temperatura: {dados['main']['temp']}")
    return dados

if __name__ == "__main__":
    pipeline_clima()
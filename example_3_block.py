import requests
from prefect import flow, task
from prefect.blocks.system import Secret



@task
def login() -> str:
    usuario = Secret.load("fakestore-user").get()      # credencial vem do Block, não do código
    senha = Secret.load("fakestore-pass").get()
    resp = requests.post(
        "https://fakestoreapi.com/auth/login",
        json={"username": usuario, "password": senha},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["token"]

@flow(log_prints=True)
def pipeline_auth():
    token = login()
    print(f"Token obtido: {token[:20]}...")
    return token

if __name__ == "__main__":
    pipeline_auth()
import json
from pathlib import Path
import requests
from prefect import flow, task

PASTA = Path(__file__).parent / "dados"
PASTA.mkdir(exist_ok=True)
ARQUIVO = PASTA / "resumo_produtos.json"

@task
def buscar_produto(pid: int) -> dict:
    r = requests.get(f"https://fakestoreapi.com/products/{pid}", timeout=10)
    r.raise_for_status()
    return r.json()

@task
def consolidar(produtos: list) -> dict:
    precos = [p["price"] for p in produtos]
    return {"qtd": len(produtos), "preco_medio": round(sum(precos) / len(precos), 2)}

@flow(log_prints=True)
def ingestao_produtos():
    produtos = buscar_produto.map([1, 2, 3, 4, 5])
    resumo = consolidar(produtos)
    ARQUIVO.write_text(json.dumps(resumo, ensure_ascii=False))
    print(f"Ingestão concluída: {resumo}")

if __name__ == "__main__":
    ingestao_produtos()
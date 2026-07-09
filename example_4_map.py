import requests
from prefect import flow, task

@task
def buscar_produto(produto_id: int) -> dict:
    resp = requests.get(f"https://fakestoreapi.com/products/{produto_id}", timeout=10)
    resp.raise_for_status()
    return resp.json()

@task
def extrair_preco(produto: dict) -> float:
    return produto["price"]

@task
def consolidar(precos: list) -> dict:
    # FAN-IN: recebe a lista inteira e reduz a um resultado
    return {
        "qtd_produtos": len(precos),
        "preco_total": round(sum(precos), 2),
        "preco_medio": round(sum(precos) / len(precos), 2),
        "mais_caro": max(precos),
    }

@flow(log_prints=True)
def pipeline_precos():
    ids = [1, 2, 3, 4, 5]

    # FAN-OUT: uma task run por produto (concorrente)
    produtos = buscar_produto.map(ids)

    # FAN-OUT: uma task run por preço (concorrente)
    precos = extrair_preco.map(produtos)

    # FAN-IN: uma task só, que recebe TODOS os preços de uma vez
    resumo = consolidar(precos)

    print(resumo)
    return resumo

if __name__ == "__main__":
    pipeline_precos()
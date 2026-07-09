import json
from pathlib import Path
from prefect import flow

ARQUIVO = Path(__file__).parent / "dados" / "resumo_produtos.json"

@flow(log_prints=True)
def gerar_relatorio():
    resumo = json.loads(ARQUIVO.read_text())
    print(f"=== RELATÓRIO ===\nProdutos: {resumo['qtd']} | médio: R$ {resumo['preco_medio']}")

if __name__ == "__main__":
    gerar_relatorio()
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class ValidarProdutosOperator(BaseOperator):
    """
    Valida o schema básico dos produtos retornados pela FakeStore API.
    """

    template_fields = ("products",)

    def __init__(self, products, **kwargs):
        super().__init__(**kwargs)
        self.products = products

    def execute(self, context):
        if not isinstance(self.products, list):
            raise AirflowException("A resposta da API não é uma lista de produtos.")

        required_fields = {"id", "title", "price", "category"}

        for product in self.products:
            missing_fields = required_fields - set(product.keys())

            if missing_fields:
                raise AirflowException(
                    f"Produto inválido. Campos ausentes: {missing_fields}. Produto: {product}"
                )

            if not isinstance(product["price"], (int, float)):
                raise AirflowException(
                    f"Preço inválido no produto {product.get('id')}: {product.get('price')}"
                )

            if not product["category"]:
                raise AirflowException(
                    f"Categoria vazia no produto {product.get('id')}"
                )

        self.log.info("Validação concluída. Total de produtos válidos: %s", len(self.products))

        return self.products

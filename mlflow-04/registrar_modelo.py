import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_experiment("wine-classification")

runs = mlflow.search_runs(
    experiment_names=["wine-classification"],
    order_by=["metrics.accuracy DESC"],
)

melhor = runs.iloc[0]
run_id = melhor["run_id"]
accuracy = melhor["metrics.accuracy"]
nome = melhor["tags.mlflow.runName"]

print(f"Melhor run: {nome}")
print(f" accuracy : {accuracy:.3f}")
print(f" run_id   : {run_id}")

model_uri = f"runs:/{run_id}/model"
model_name = "wine-classifier-best"

print(f"\nRegistrando '{model_name}' no Model Registry...")

model_version = mlflow.register_model(
    model_uri=model_uri,
    name=model_name,
)

print(f" Versão criada: v{model_version.version}")

client = MlflowClient()

client.set_registered_model_alias(
    name=model_name,
    alias="champion",
    version=model_version.version,
)

print(f" Alias @champion → versão {model_version.version}")

print("\nPara carregar o modelo campeão em qualquer lugar:")
print(f" mlflow.sklearn.load_model('models:/{model_name}@champion')")

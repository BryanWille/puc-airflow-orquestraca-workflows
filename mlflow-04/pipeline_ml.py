import argparse
import mlflow
import mlflow.sklearn
import numpy as np

from sklearn.datasets import load_wine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score

parser = argparse.ArgumentParser()
parser.add_argument("--n_estimators", type=int, default=100)
parser.add_argument("--max_depth", type=int, default=5)
args = parser.parse_args()

mlflow.set_experiment("wine-classification")

wine = load_wine()

X_train, X_test, y_train, y_test = train_test_split(
    wine.data,
    wine.target,
    test_size=0.2,
    random_state=42,
)

print(f"Treino: {X_train.shape[0]} amostras | Teste: {X_test.shape[0]} amostras")

modelos = {
    "LogisticRegression": {
        "class": LogisticRegression,
        "params": {"max_iter": 200, "random_state": 42},
    },
    "RandomForest": {
        "class": RandomForestClassifier,
        "params": {
            "max_depth": args.max_depth,
            "n_estimators": args.n_estimators,
            "random_state": 42,
        },
    },
    "GradientBoosting": {
        "class": GradientBoostingClassifier,
        "params": {
            "n_estimators": 100,
            "learning_rate": 0.1,
            "random_state": 42,
        },
    },
}

for nome, config in modelos.items():
    print(f"\nTreinando {nome}...")

    with mlflow.start_run(run_name=nome):
        for k, v in config["params"].items():
            mlflow.log_param(k, v)

        model = config["class"](**config["params"])
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average="weighted")

        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1_score", f1)

        mlflow.sklearn.log_model(model, name="model")

        run_id = mlflow.active_run().info.run_id
        print(f" accuracy={acc:.3f} | f1={f1:.3f} | run_id={run_id}")

print("\nTreino concluído. Acesse http://localhost:5000 para ver os runs.")

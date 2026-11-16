import json

import joblib
import pandas as pd
import pytest
from sklearn.datasets import load_iris
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.tree import DecisionTreeClassifier

from iris_project.data_exporters.save_results import save_results
from iris_project.transformers.evaluate_model import evaluate_model
from iris_project.transformers.prepare_data import transform
from iris_project.transformers.train_model import (
    test_accuracy as mage_test_accuracy,
    train_model,
)


def _prepared_data():
    return transform(load_iris(as_frame=True).frame)


def test_decision_tree_trains_on_exact_split_and_evaluates_reproducibly():
    data = _prepared_data()

    first = train_model(data, max_depth=3)
    second = train_model(data, max_depth=3)

    assert first["train_rows"] == 120
    assert first["test_rows"] == 30
    assert first["max_depth"] == 3
    assert isinstance(first["model"], DecisionTreeClassifier)
    assert first["accuracy"] == second["accuracy"]
    assert first["classification_report"] == second["classification_report"]
    assert len(first["test_target"]) == 30
    assert first["actual_labels"] == first["test_target"]
    assert len(first["predictions"]) == 30
    assert first["accuracy"] == accuracy_score(
        first["test_target"],
        first["predictions"],
    )


@pytest.mark.parametrize("max_depth", [1, 2, 3, 4, 5])
def test_runtime_max_depth_accepts_only_supported_values(max_depth):
    result = train_model(_prepared_data(), max_depth=max_depth)

    assert result["model"].max_depth == max_depth


@pytest.mark.parametrize("max_depth", [0, 6, -1, 1.5, "3", True])
def test_runtime_max_depth_rejects_invalid_values(max_depth):
    with pytest.raises(ValueError, match="max_depth must be an integer"):
        train_model(_prepared_data(), max_depth=max_depth)


def test_max_depth_can_come_from_mage_block_configuration():
    result = train_model(
        _prepared_data(),
        configuration={"max_depth": 2},
    )

    assert result["model"].max_depth == 2


def test_accuracy_gate_rejects_incorrect_held_out_accuracy():
    evaluation = train_model(_prepared_data(), max_depth=3)
    evaluation["accuracy"] = 0.0

    with pytest.raises(AssertionError, match="Reported accuracy"):
        mage_test_accuracy(evaluation)


def test_accuracy_gate_accepts_low_accuracy_when_report_matches_test_predictions():
    evaluation = train_model(_prepared_data(), max_depth=1)
    assert evaluation["accuracy"] < 0.9

    mage_test_accuracy(evaluation)


def test_train_model_removes_stale_predictions_before_attempting_run(tmp_path):
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    predictions_path = results_dir / "iris_predictions.csv"
    predictions_path.write_text("stale output", encoding="utf-8")

    evaluation = train_model(
        _prepared_data(),
        max_depth=1,
        repo_path=str(tmp_path),
    )

    assert evaluation["accuracy"] < 0.9
    assert not predictions_path.exists()


def test_save_results_persists_model_and_evaluation(tmp_path):
    evaluation = train_model(_prepared_data(), max_depth=3)

    result = save_results(evaluation, repo_path=str(tmp_path))

    model_path = tmp_path / "models" / "iris_decision_tree.joblib"
    results_path = tmp_path / "results" / "iris_evaluation.json"
    predictions_path = tmp_path / "results" / "iris_predictions.csv"
    assert model_path.exists()
    assert results_path.exists()
    assert predictions_path.exists()
    assert isinstance(joblib.load(model_path), DecisionTreeClassifier)
    saved_results = json.loads(results_path.read_text(encoding="utf-8"))
    assert saved_results["accuracy"] == evaluation["accuracy"]
    assert saved_results["train_rows"] == 120
    assert saved_results["test_rows"] == 30
    predictions = pd.read_csv(predictions_path)
    assert len(predictions) == 30
    assert predictions["actual"].tolist() == evaluation["test_target"]
    assert predictions["predicted"].tolist() == evaluation["predictions"]
    assert result["model_path"] == str(model_path)
    assert result["predictions_path"] == str(predictions_path)

    predictions_path.write_text("stale output", encoding="utf-8")
    save_results(evaluation, repo_path=str(tmp_path))
    overwritten_predictions = pd.read_csv(predictions_path)
    assert len(overwritten_predictions) == 30
    assert overwritten_predictions["predicted"].tolist() == evaluation["predictions"]


def test_evaluate_model_reports_and_persists_held_out_metrics(tmp_path):
    evaluation = train_model(_prepared_data(), max_depth=3)

    result = evaluate_model(evaluation, repo_path=str(tmp_path))

    expected_matrix = confusion_matrix(
        evaluation["test_target"],
        evaluation["predictions"],
        labels=[0, 1, 2],
    )
    expected_report = classification_report(
        evaluation["test_target"],
        evaluation["predictions"],
        labels=[0, 1, 2],
        output_dict=True,
        zero_division=0,
    )
    assert result["confusion_matrix"] == expected_matrix.tolist()
    assert result["labels"] == [0, 1, 2]
    assert result["accuracy"] == accuracy_score(
        evaluation["test_target"],
        evaluation["predictions"],
    )
    metric_rows = {row["class"]: row for row in result["metrics_table"]}
    assert metric_rows["macro avg"]["precision"] == expected_report["macro avg"]["precision"]
    assert metric_rows["weighted avg"]["recall"] == expected_report["weighted avg"]["recall"]
    assert len(metric_rows) == 6
    confusion_path = tmp_path / "results" / "iris_confusion_matrix.csv"
    metrics_path = tmp_path / "results" / "iris_metrics.csv"
    evaluation_path = tmp_path / "results" / "iris_model_evaluation.json"
    assert confusion_path.exists()
    assert metrics_path.exists()
    assert evaluation_path.exists()
    assert pd.read_csv(confusion_path, index_col="actual").shape == (3, 3)
    assert len(pd.read_csv(metrics_path)) == 6
    saved_evaluation = json.loads(evaluation_path.read_text(encoding="utf-8"))
    assert saved_evaluation["confusion_matrix"] == expected_matrix.tolist()
    assert saved_evaluation["evaluation_path"] == str(evaluation_path)
    assert saved_evaluation["metrics_table"][-1]["precision"] is None


def test_pipeline_metadata_connects_both_branches_to_train_model():
    import yaml
    from pathlib import Path

    metadata_path = (
        Path(__file__).resolve().parents[1]
        / "iris_project"
        / "pipelines"
        / "iris_classifier"
        / "metadata.yaml"
    )
    pipeline = yaml.safe_load(metadata_path.read_text(encoding="utf-8"))
    blocks = {block["uuid"]: block for block in pipeline["blocks"]}

    assert blocks["train_model"]["downstream_blocks"] == [
        "save_results",
        "evaluate_model",
    ]
    assert blocks["save_results"]["upstream_blocks"] == ["train_model"]
    assert blocks["evaluate_model"]["upstream_blocks"] == ["train_model"]
    assert "conditionals" not in pipeline or not pipeline["conditionals"] 
# Iris classifier pipeline

## Architecture

The `iris_classifier` pipeline loads all 150 Iris records, creates a deterministic
80:20 train/test split (`random_state=42`), and trains a Decision Tree with
`random_state=42`.

```text
load_iris -> prepare_data -> train_model -> save_results
                                      \-> evaluate_model
```

- `load_iris` loads the scikit-learn dataset.
- `prepare_data` removes incomplete rows and labels the fixed train/test split.
- `train_model` validates `max_depth` (integer 1–5), fits on the 120 training
  rows, evaluates on the 30 held-out rows, and returns the model, accuracy,
  classification report, actual labels, predictions, and test metadata.
- `save_results` saves the model, evaluation summary, and held-out predictions.
- `evaluate_model` independently predicts the held-out records, checks those
  predictions against the training output, and saves a confusion matrix and
  classification metrics.

Mage runs the `@test` functions in `train_model` before scheduling downstream
blocks. The `test_accuracy` test checks that accuracy matches the held-out
labels and predictions and that the classification report agrees. A failed
test stops the pipeline before either downstream branch can run. At the start
of each training attempt, an old prediction CSV is removed so a failed run
cannot leave stale predictions looking current.

## Runtime variable and commands

`max_depth` is a pipeline runtime variable. Its default is `3`; override it
with Mage's `--runtime-vars` option. Run each command from the repository root
in PowerShell:

```powershell
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 1}' /home/src/iris_project iris_classifier
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 2}' /home/src/iris_project iris_classifier
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 3}' /home/src/iris_project iris_classifier
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 4}' /home/src/iris_project iris_classifier
docker exec freshcart-mage mage run --test --runtime-vars '{"max_depth": 5}' /home/src/iris_project iris_classifier
```

`--test` enables Mage's block tests. The `iris_predictions.csv` file is removed
by `train_model` for every attempted execution and recreated by `save_results`
only after the upstream training tests pass.

## Generated files

All run artifacts are written beneath `iris_project/`:

- `models/iris_decision_tree.joblib` — fitted model.
- `results/iris_evaluation.json` — accuracy, classification report, split sizes,
  depth, and model/prediction paths.
- `results/iris_predictions.csv` — 30 held-out rows with source index, features,
  actual target, and predicted target.
- `results/iris_confusion_matrix.csv` — labeled 3×3 confusion matrix.
- `results/iris_metrics.csv` — per-class, macro, weighted, and accuracy metrics.
- `results/iris_model_evaluation.json` — structured evaluation results and
  artifact paths.
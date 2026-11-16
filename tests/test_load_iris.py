from pathlib import Path
from runpy import run_path


def test_load_iris_uses_mage_injected_decorator():
    decorated_functions = []

    def mage_data_loader(function):
        decorated_functions.append(function)
        return function

    block_path = (
        Path(__file__).resolve().parents[1]
        / "iris_project"
        / "data_loaders"
        / "load_iris.py"
    )
    run_path(str(block_path), init_globals={"data_loader": mage_data_loader})

    assert len(decorated_functions) == 1
    assert decorated_functions[0].__name__ == "load_iris"
    assert decorated_functions[0]().shape == (150, 5)
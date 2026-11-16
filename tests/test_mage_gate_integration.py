import json
import subprocess
import tempfile
import textwrap
import uuid
from pathlib import Path

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
MAGE_CONTAINER = "freshcart-mage"


def _container_project_path(project_path):
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{range .Mounts}}{{.Source}}|{{.Destination}}{{println}}{{end}}",
            MAGE_CONTAINER,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        pytest.skip(f"Mage container {MAGE_CONTAINER!r} is not available.")

    for line in result.stdout.splitlines():
        source, destination = line.split("|", 1)
        try:
            relative_path = project_path.resolve().relative_to(Path(source).resolve())
        except ValueError:
            continue
        return f"{destination.rstrip('/')}/{relative_path.as_posix()}"

    pytest.skip("Mage container does not mount the temporary project directory.")


def test_failed_train_model_test_prevents_save_results_from_running():
    project_id = f"mage_gate_{uuid.uuid4().hex[:12]}"
    with tempfile.TemporaryDirectory(
        prefix=".mage-gate-",
        dir=REPOSITORY_ROOT,
    ) as temporary_directory:
        project_path = Path(temporary_directory)
        container_project_path = _container_project_path(project_path)
        sentinel_path = project_path / "save_results_was_executed.txt"
        container_sentinel_path = f"{container_project_path}/save_results_was_executed.txt"

        (project_path / "pipelines" / project_id).mkdir(parents=True)
        (project_path / "transformers").mkdir()
        (project_path / "data_exporters").mkdir()
        (project_path / "metadata.yaml").write_text(
            f"project_uuid: {project_id}\n",
            encoding="utf-8",
        )
        (project_path / "pipelines" / project_id / "__init__.py").write_text(
            "",
            encoding="utf-8",
        )
        (project_path / "transformers" / "train_model.py").write_text(
            textwrap.dedent(
                """
                @transformer
                def train_model(*args, **kwargs):
                    return {"accuracy": 0.0}

                @test
                def test_accuracy(output, *args):
                    assert output["accuracy"] >= 0.9
                """
            ),
            encoding="utf-8",
        )
        (project_path / "data_exporters" / "save_results.py").write_text(
            textwrap.dedent(
                f"""
                @data_exporter
                def save_results(evaluation, *args, **kwargs):
                    with open({container_sentinel_path!r}, "w") as file:
                        file.write("executed")
                """
            ),
            encoding="utf-8",
        )
        block_template = textwrap.indent(
            textwrap.dedent(
                """\
                - all_upstream_blocks_executed: {all_upstream}
                  color: null
                  configuration: {{}}
                  downstream_blocks: {downstream}
                  executor_config: null
                  executor_type: local_python
                  has_callback: false
                  language: python
                  name: {name}
                  retry_config: null
                  status: not_executed
                  timeout: null
                  type: {block_type}
                  upstream_blocks: {upstream}
                  uuid: {name}
                """
            ),
            "  ",
        )
        metadata = (
            "blocks:\n"
            + block_template.format(
                all_upstream="true",
                downstream="[save_results]",
                name="train_model",
                block_type="transformer",
                upstream="[]",
            )
            + block_template.format(
                all_upstream="false",
                downstream="[]",
                name="save_results",
                block_type="data_exporter",
                upstream="[train_model]",
            )
            + textwrap.dedent(
                f"""\
                cache_block_output_in_memory: false
                callbacks: []
                concurrency_config: {{}}
                conditionals: []
                name: {project_id}
                type: python
                uuid: {project_id}
                """
            )
        )
        (project_path / "pipelines" / project_id / "metadata.yaml").write_text(
            metadata,
            encoding="utf-8",
        )

        result = subprocess.run(
            [
                "docker",
                "exec",
                MAGE_CONTAINER,
                "mage",
                "run",
                "--test",
                "--runtime-vars",
                json.dumps({"max_depth": 1}),
                container_project_path,
                project_id,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=120,
        )

        output = result.stdout + result.stderr
        assert result.returncode != 0, output
        assert "FAIL: test_accuracy" in output, output
        assert not sentinel_path.exists()
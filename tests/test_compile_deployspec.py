import pytest
import os
from core_framework.models import TaskPayload, DeploySpec
from core_deployspec.compiler import load_deployspec, compile_deployspec


@pytest.fixture
def task_payload():
    """Mock task payload for testing."""

    path = os.path.dirname(os.path.abspath(__file__))
    package_file = os.path.join(path, "package.zip")

    return TaskPayload(
        **{
            "Task": "deploy",
            "DeploymentDetails": {
                "Client": "test-client",
                "Portfolio": "test-portfolio",
                "App": "test-app",
                "Branch": "main",
                "Build": "latest",
                "Environment": "prod",
            },
            "Package": {
                "bucket_name": "test-bucket",
                "key": package_file,
                "bucket_region": "us-west-2",
            },
        }
    )


def test_compile_deployspec_success(task_payload: TaskPayload):
    """Test successful compilation of deployspec."""
    deployspecs = load_deployspec(task_payload)
    assert deployspecs is not None, "Should load deployspecs successfully"

    for task, deployspec in deployspecs.items():

        assert isinstance(task, str), f"Task {task} should be a string"

        assert isinstance(deployspec, DeploySpec), f"Deployspec for {task} should be an instance of DeploySpec"
        assert isinstance(deployspec.actions, list), f"Actions for {task} should be a list"

        task_payload.task = task
        task_payload.package.deployspec = deployspec

        compiled_actions = compile_deployspec(task_payload)

        assert compiled_actions is not None, "Should compile actions successfully"
        assert isinstance(compiled_actions, list), "Compiled actions should be a dictionary"

        assert len(compiled_actions) == 27, "Compiled actions should not be 27 actions from our data"

import os
import pytest
from unittest.mock import patch, Mock
import tempfile

import core_framework as util
from core_framework.models import DeploySpec, ActionSpec, TaskPayload
from core_deployspec.compiler import load_deployspec


@pytest.fixture
def test_data_dir():
    """Get the test data directory path."""
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def task_payload():
    """Mock task payload for testing."""
    return TaskPayload(
        **{
            "Task": "deploy",
            "DeploymentDetails": {
                "Portfolio": "test-portfolio",
                "App": "test-app",
                "Environment": "test-env",
                "Branch": "main",
                "Build": "latest",
            },
        }
    )


def test_load_deployspec_yaml_format(test_data_dir, task_payload: TaskPayload):
    """Test loading deployspec from YAML format."""

    file = os.path.join(test_data_dir, "deployspec.yaml")
    task_payload.package.key = file

    deployspecs = load_deployspec(task_payload)

    assert deployspecs is not None, "Should load deployspecs successfully"

    deployspec = deployspecs.get("deploy")

    assert deployspec is not None, "Should load deployspec successfully"

    assert isinstance(deployspec, DeploySpec), "Should return a DeploySpec instance"
    assert isinstance(
        deployspec.action_specs, list
    ), "DeploySpec actions should be a list"

    assert (
        len(deployspec.actions) == 3
    ), "There should be 6 actions in the sample deployspec.yaml"

    # Test first action spec
    action_spec = deployspec.actions[0]

    assert isinstance(
        action_spec, ActionSpec
    ), "ActionSpec should be an instance of ActionSpec"
    assert isinstance(action_spec.params, dict), "ActionSpec params should be a dict"


def test_load_deployspec_json_format(test_data_dir, task_payload: TaskPayload):
    """Test loading deployspec from JSON format."""

    file = os.path.join(test_data_dir, "deployspec.json")
    task_payload.package.key = file

    deployspecs = load_deployspec(task_payload)

    assert deployspecs is not None, "Should load deployspecs successfully"

    deployspec = deployspecs.get("deploy")

    assert deployspec is not None, "Should load deployspec successfully"

    assert isinstance(deployspec, DeploySpec), "Should return a DeploySpec instance"
    assert isinstance(
        deployspec.action_specs, list
    ), "DeploySpec actions should be a list"

    assert (
        len(deployspec.actions) == 3
    ), "There should be 6 actions in the sample deployspec.json"

    # Test first action spec
    action_spec = deployspec.actions[0]

    assert isinstance(
        action_spec, ActionSpec
    ), "ActionSpec should be an instance of ActionSpec"
    assert isinstance(action_spec.params, dict), "ActionSpec params should be a dict"


def test_load_deployspec_json_error_handling(test_data_dir, task_payload: TaskPayload):
    """Test error handling when JSON loading fails."""
    file = os.path.join(test_data_dir, "deployspec.json")
    task_payload.package.key = file

    # Patch the json.load function to throw an exception
    with patch(
        "json.load",
        side_effect=ValueError("Error loading deployspec"),
    ):
        data = load_deployspec(task_payload)
        assert data is None, "Should return None if deployspec cannot be loaded"


def test_load_deployspec_current_directory_no_file(
    test_data_dir, task_payload: TaskPayload
):
    """Test loading from current directory when no deployspec exists."""
    # Use the deployspec_none directory that should not have deployspec files
    file = os.path.join(test_data_dir, "deployspec.none")
    task_payload.package.key = file

    # Ensure the directory exists
    os.makedirs(file, exist_ok=True)

    deployspec = load_deployspec(task_payload)
    assert (
        deployspec is None
    ), "Should return None if no deployspec is found in the directory"


def test_load_deployspec_invalid_directory(task_payload: TaskPayload):
    """Test behavior with invalid directory path."""
    invalid_path = "/path/that/does/not/exist"
    task_payload.package.key = invalid_path

    deployspec = load_deployspec(invalid_path)

    assert deployspec is None, "Should return None for invalid directory path"


def test_load_deployspec_default_current_directory(
    test_data_dir, task_payload: TaskPayload
):
    """Test loading deployspec from current directory (no path specified)."""

    # Change to test directory that has deployspec
    yaml_folder = os.path.join(test_data_dir, "deployspec.yaml")
    task_payload.package.key = yaml_folder

    # Load without specifying path (should use current directory)
    deployspecs = load_deployspec(task_payload)

    assert deployspecs is not None, "Should load deployspec from current directory"

    deployspec = deployspecs.get("deploy")

    assert deployspec is not None, "Should load deployspec from current directory"
    assert isinstance(deployspec, DeploySpec), "Should return a DeploySpec instance"

    assert (
        len(deployspec.actions) == 3
    ), "Should load the correct deployspec from current directory"

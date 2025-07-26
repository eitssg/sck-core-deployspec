import os
import pytest
from unittest.mock import patch, Mock
import tempfile
import shutil

import core_framework as util
from core_framework.models import DeploySpec, ActionSpec
import core_deployspec.compiler as compiler


class TestLoadDeployspec:
    """Test class for load_deployspec functionality."""

    @pytest.fixture
    def test_data_dir(self):
        """Get the test data directory path."""
        return os.path.dirname(os.path.abspath(__file__))

    def test_load_deployspec_yaml_format(self, test_data_dir):
        """Test loading deployspec from YAML format."""
        current_folder = os.path.join(test_data_dir, "deployspec_yaml")
        deployspec = compiler.load_deployspec(current_folder)

        assert deployspec is not None, "Should load deployspec successfully"
        assert len(deployspec) == 6, "There should be 6 actions in the sample deployspec.yaml"

        # Test DeploySpec creation
        ds = DeploySpec(actions=deployspec)
        assert isinstance(ds, DeploySpec), "Should return a DeploySpec instance"
        assert isinstance(ds.action_specs, list), "DeploySpec actions should be a list"

        # Test first action spec
        action_spec = ds.action_specs[0]
        assert isinstance(action_spec, ActionSpec), "ActionSpec should be an instance of ActionSpec"
        assert isinstance(action_spec.params, dict), "ActionSpec params should be a dict"

    def test_load_deployspec_json_format(self, test_data_dir):
        """Test loading deployspec from JSON format."""
        current_folder = os.path.join(test_data_dir, "deployspec_json")
        deployspec = compiler.load_deployspec(current_folder)

        assert deployspec is not None, "Should load deployspec successfully"

        # Test DeploySpec creation
        ds = DeploySpec(actions=deployspec)
        assert isinstance(ds, DeploySpec), "Should return a DeploySpec instance"
        assert isinstance(ds.action_specs, list), "DeploySpec actions should be a list"

        # Test first action spec
        action_spec = ds.action_specs[0]
        assert isinstance(action_spec, ActionSpec), "ActionSpec should be an instance of ActionSpec"
        assert isinstance(action_spec.params, dict), "ActionSpec params should be a dict"

    def test_load_deployspec_struct_format(self, test_data_dir):
        """Test loading deployspec from struct format."""
        current_folder = os.path.join(test_data_dir, "deployspec_struct")
        deployspec = compiler.load_deployspec(current_folder)

        assert deployspec is not None, "Should load deployspec successfully"

        # Test DeploySpec creation
        ds = DeploySpec(actions=deployspec)
        assert isinstance(ds, DeploySpec), "Should return a DeploySpec instance"
        assert isinstance(ds.action_specs, list), "DeploySpec actions should be a list"
        assert len(ds.action_specs) == 1, "There should be 1 action in the deployspec_struct/deployspec.yaml"
        assert isinstance(ds.action_specs[0], ActionSpec), "ActionSpec should be an instance of ActionSpec"

    def test_load_deployspec_json_error_handling(self, test_data_dir):
        """Test error handling when JSON loading fails."""
        current_folder = os.path.join(test_data_dir, "deployspec_json")

        # Patch the json.load function to throw an exception
        with patch("core_framework.common.json.load", side_effect=ValueError("Error loading deployspec")):
            data = compiler.load_deployspec(current_folder)
            assert data is None, "Should return None if deployspec cannot be loaded"

    def test_load_deployspec_no_file_found(self):
        """Test behavior when no deployspec is found."""
        # Create a temporary empty directory
        with tempfile.TemporaryDirectory() as temp_dir:
            deployspec = util.load_deployspec(temp_dir)
            assert deployspec is None, "Should return None if no deployspec is found in the directory"

    def test_load_deployspec_current_directory_no_file(self, test_data_dir):
        """Test loading from current directory when no deployspec exists."""
        # Use the deployspec_none directory that should not have deployspec files
        none_folder = os.path.join(test_data_dir, "deployspec_none")

        # Ensure the directory exists
        os.makedirs(none_folder, exist_ok=True)

        deployspec = util.load_deployspec(none_folder)
        assert deployspec is None, "Should return None if no deployspec is found in the directory"

    def test_load_deployspec_invalid_directory(self):
        """Test behavior with invalid directory path."""
        invalid_path = "/path/that/does/not/exist"
        deployspec = compiler.load_deployspec(invalid_path)
        assert deployspec is None, "Should return None for invalid directory path"

    def test_load_deployspec_default_current_directory(self, test_data_dir):
        """Test loading deployspec from current directory (no path specified)."""
        # Save current directory
        original_cwd = os.getcwd()

        try:
            # Change to test directory that has deployspec
            yaml_folder = os.path.join(test_data_dir, "deployspec_yaml")
            os.chdir(yaml_folder)

            # Load without specifying path (should use current directory)
            deployspec = compiler.load_deployspec()
            assert deployspec is not None, "Should load deployspec from current directory"
            assert len(deployspec) == 6, "Should load the correct deployspec from current directory"

        finally:
            # Always restore original directory
            os.chdir(original_cwd)

    @pytest.mark.parametrize(
        "folder_name,expected_actions",
        [
            ("deployspec_yaml", 6),
            ("deployspec_struct", 1),
            ("deployspec_json", None),  # Adjust based on actual content
        ],
    )
    def test_load_deployspec_various_formats(self, test_data_dir, folder_name, expected_actions):
        """Parameterized test for different deployspec formats."""
        current_folder = os.path.join(test_data_dir, folder_name)

        # Skip if folder doesn't exist
        if not os.path.exists(current_folder):
            pytest.skip(f"Test folder {folder_name} does not exist")

        deployspec = util.load_deployspec(current_folder)

        if expected_actions is None:
            assert deployspec is not None, f"Should load deployspec from {folder_name}"
        else:
            assert deployspec is not None, f"Should load deployspec from {folder_name}"
            assert len(deployspec) == expected_actions, f"Should have {expected_actions} actions in {folder_name}"


# Additional integration test
def test_deployspec_end_to_end_workflow():
    """Test the complete workflow of loading and creating DeploySpec."""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_folder = os.path.join(test_data_dir, "deployspec_yaml")

    # Load deployspec
    deployspec_data = compiler.load_deployspec(yaml_folder)
    assert deployspec_data is not None, "Should load deployspec data"

    # Create DeploySpec object
    deploy_spec = DeploySpec(actions=deployspec_data)
    assert isinstance(deploy_spec, DeploySpec), "Should create DeploySpec instance"

    # Validate all action specs
    for i, action_spec in enumerate(deploy_spec.action_specs):
        assert isinstance(action_spec, ActionSpec), f"Action {i} should be ActionSpec instance"
        assert hasattr(action_spec, "params"), f"Action {i} should have params"
        assert isinstance(action_spec.params, dict), f"Action {i} params should be dict"

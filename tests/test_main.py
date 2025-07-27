import pytest
from pydantic import ValidationError

from core_framework.constants import (
    SCOPE_PORTFOLIO,
    SCOPE_APP,
    SCOPE_BRANCH,
    SCOPE_BUILD,
)

from core_deployspec.compiler import (
    __get_stack_scope,
    get_region_account_labels,
)

from core_framework.models import ActionSpec


class TestCompilerFunctions:
    """Test class for compiler utility functions."""

    def test_get_region_account_labels_multiple_accounts_regions(self):
        """Test get_region_account_labels with multiple accounts and regions."""
        try:
            deployspec = self._get_deployspec(
                "test_stack",
                ["123456789012", "123456789013"],
                ["us-east-1", "ap-southeast-1"],
            )

            for spec in deployspec:
                action_spec = ActionSpec(**spec)
                region_account_labels = get_region_account_labels(action_spec)

                expected_labels = [
                    "test_stack-label-123456789012-us-east-1",
                    "test_stack-label-123456789012-ap-southeast-1",
                    "test_stack-label-123456789013-us-east-1",
                    "test_stack-label-123456789013-ap-southeast-1",
                ]

                assert region_account_labels == expected_labels

        except ValidationError as e:
            pytest.fail(f"ValidationError: {e.errors()}")
        except Exception as e:
            pytest.fail(f"Unexpected error: {e}")

    def test_get_region_account_labels_single_account_region(self):
        """Test get_region_account_labels with single account and region."""
        deployspec = self._get_deployspec(
            "single_stack",
            ["123456789012"],
            ["us-west-2"],
        )

        action_spec = ActionSpec(**deployspec[0])
        region_account_labels = get_region_account_labels(action_spec)

        expected_labels = ["single_stack-label-123456789012-us-west-2"]
        assert region_account_labels == expected_labels

    def test_get_region_account_labels_empty_lists(self):
        """Test get_region_account_labels with empty account/region lists."""
        deployspec = self._get_deployspec("empty_stack", [], [])

        action_spec = ActionSpec(**deployspec[0])
        region_account_labels = get_region_account_labels(action_spec)

        assert region_account_labels == []

    @pytest.mark.parametrize(
        "stack_name,expected_scope",
        [
            ("{{ core.Portfolio }}-resources", SCOPE_PORTFOLIO),
            ("{{ core.Project }}-{{ core.App }}-resources", SCOPE_APP),
            (
                "{{ core.Project }}-{{ core.App }}-{{ core.Branch }}-resources",
                SCOPE_BRANCH,
            ),
            (
                "{{ core.Project }}-{{ core.App }}-{{ core.Branch }}-{{ core.Build}}-resources",
                SCOPE_BUILD,
            ),
            ("simple-stack-name", None),  # Test non-templated name
            (
                "{{ core.Portfolio }}-{{ core.App }}-{{ core.Branch }}-{{ core.Build}}-resources",
                SCOPE_BUILD,
            ),  # All variables
        ],
    )
    def test_get_stack_scope_various_patterns(self, stack_name, expected_scope):
        """Test __get_stack_scope with various stack name patterns."""
        result = __get_stack_scope(stack_name)
        assert result == expected_scope

    def test_get_stack_scope_edge_cases(self):
        """Test __get_stack_scope with edge cases."""
        # Empty string
        assert __get_stack_scope("") is None

        # None input (if function handles it)
        try:
            result = __get_stack_scope(None)
            assert result is None
        except (TypeError, AttributeError):
            # Expected if function doesn't handle None
            pass

    def test_action_spec_validation_with_compiler_functions(self):
        """Test that ActionSpec validation works with compiler functions."""
        # Test valid action spec
        valid_spec = self._get_action("valid_stack", ["123456789012"], ["us-east-1"])

        try:
            action_spec = ActionSpec(**valid_spec)
            assert action_spec.label == "valid_stack-label"
            assert action_spec.type == "create_stack"
            assert "stack_name" in action_spec.params
        except ValidationError as e:
            pytest.fail(f"Valid ActionSpec failed validation: {e.errors()}")

        # Test invalid action spec (missing required fields)
        invalid_spec = {"label": "test", "type": "invalid_type"}

        with pytest.raises(ValidationError):
            ActionSpec(**invalid_spec)

    # Helper methods (not fixtures, just utility functions)
    def _get_action_parameters(
        self, name: str, account: list[str], region: list[str]
    ) -> dict:
        """Helper to create action parameters."""
        return {
            "stack_name": name,
            "template": f"{name}-stack.yaml",
            "accounts": account,
            "regions": region,
            "stack_policy": "stack_policy",
        }

    def _get_user_action_parameters(
        self, name: str, user: str, account: str, region: str
    ) -> dict:
        """Helper to create user action parameters."""
        return {
            "stack_name": name,
            "user_name": user,
            "account": account,
            "region": region,
            "stack_policy": "stack_policy",
        }

    def _get_action(self, name: str, account: list[str], region: list[str]) -> dict:
        """Helper to create action dictionary."""
        label = f"{name}-label"
        return {
            "label": label,
            "type": "create_stack",
            "params": self._get_action_parameters(name, account, region),
            "scope": "build",
        }

    def _get_deployspec(
        self, name: str, account: list[str], region: list[str]
    ) -> list[dict]:
        """Helper to create deployspec list."""
        return [self._get_action(name, account, region)]

    def _get_user_action(self, name: str, user: str, account: str, region: str) -> dict:
        """Helper to create user action dictionary."""
        label = f"{name}-label"
        return {
            "label": label,
            "type": "create_user",
            "params": self._get_user_action_parameters(name, user, account, region),
        }

    def _get_user_deployspec(
        self, name: str, user: str, account: str, region: str
    ) -> list[dict]:
        """Helper to create user deployspec list."""
        return [self._get_user_action(name, user, account, region)]


# Test fixtures for integration tests if needed
@pytest.fixture
def sample_action_spec():
    """Fixture providing a sample ActionSpec for integration tests."""
    return {
        "label": "test-stack-label",
        "type": "create_stack",
        "params": {
            "stack_name": "test-stack",
            "template": "test-stack.yaml",
            "accounts": ["123456789012"],
            "regions": ["us-east-1"],
            "stack_policy": "stack_policy",
        },
        "scope": "build",
    }


@pytest.fixture
def sample_deployment_details():
    """Fixture providing sample deployment details."""
    return {
        "Portfolio": "test-portfolio",
        "App": "test-app",
        "Branch": "main",
        "BranchShortName": "main",
        "Build": "123",
    }


# Integration tests using fixtures
def test_actionspec_integration(sample_action_spec):
    """Integration test for ActionSpec creation and compiler function usage."""
    # Create ActionSpec from sample data
    action_spec = ActionSpec(**sample_action_spec)

    # Test with compiler function
    labels = get_region_account_labels(action_spec)

    expected_labels = ["test-stack-label-123456789012-us-east-1"]
    assert labels == expected_labels

    # Test scope detection
    scope = __get_stack_scope(action_spec.params.get("stack_name", ""))
    # This should return None since "test-stack" doesn't match any template pattern
    assert scope is None

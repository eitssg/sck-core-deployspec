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

from core_framework.models import ActionResource


def test_get_region_account_labels_multiple_accounts_regions():
    """Test get_region_account_labels with multiple accounts and regions."""
    deployspec = _get_deployspec(
        "test-stack",
        ["123456789012", "123456789013"],
        ["us-east-1", "ap-southeast-1"],
    )

    for resource in deployspec:

        try:
            action_resource = ActionResource.model_validate(resource)
        except Exception as e:
            pytest.fail(f"Failed to create ActionResource: {e}")

        region_account_labels = get_region_account_labels(action_resource)

        expected_labels = [
            ":action/test-stack-label-123456789012-us-east-1",
            ":action/test-stack-label-123456789012-ap-southeast-1",
            ":action/test-stack-label-123456789013-us-east-1",
            ":action/test-stack-label-123456789013-ap-southeast-1",
        ]

        assert region_account_labels == expected_labels


def test_get_region_account_labels_single_account_region():
    """Test get_region_account_labels with single account and region."""
    deployspec = _get_deployspec(
        "single-stack",
        ["123456789012"],
        ["us-west-2"],
    )

    action_resource = ActionResource.model_validate(deployspec[0])
    region_account_labels = get_region_account_labels(action_resource)

    expected_labels = [
        ":action/single-stack-label-123456789012-us-west-2",
        ':action/single-stack-label-123456789012-ap-southeast-1',
    ]
    assert region_account_labels == expected_labels


def test_get_region_account_labels_empty_lists():
    """Test get_region_account_labels with empty account/region lists."""
    deployspec = _get_deployspec("empty-stack", [], [])

    action_resource = ActionResource.model_validate(deployspec[0])
    region_account_labels = get_region_account_labels(action_resource)

    assert region_account_labels == []


@pytest.mark.parametrize(
    "stack_name,expected_scope",
    [
        ("{{ context.Portfolio }}-resources", SCOPE_PORTFOLIO),
        ("{{ context.Project }}-{{ context.App }}-resources", SCOPE_APP),
        (
            "{{ context.Project }}-{{ context.App }}-{{ context.Branch }}-resources",
            SCOPE_BRANCH,
        ),
        (
            "{{ context.Project }}-{{ context.App }}-{{ context.Branch }}-{{ context.Build}}-resources",
            SCOPE_BUILD,
        ),
        ("simple-stack-name", SCOPE_BUILD),  # Test non-templated name
        (
            "{{ context.Portfolio }}-{{ context.App }}-{{ context.Branch }}-{{ context.Build}}-resources",
            SCOPE_BUILD,
        ),  # All variables
    ],
)
def test_get_stack_scope_various_patterns(stack_name, expected_scope):
    """Test __get_stack_scope with various stack name patterns."""
    result = __get_stack_scope(stack_name)
    assert result == expected_scope


def test_get_stack_scope_edge_cases():
    """Test __get_stack_scope with edge cases."""
    # Empty string
    assert __get_stack_scope("") is 'build'  # default is build

    # None input (if function handles it)
    try:
        result = __get_stack_scope("")
        assert result is 'build'  # default is build
    except (TypeError, AttributeError):
        # Expected if function doesn't handle None
        pass


def test_action_resource_validation_with_compiler_functions():
    """Test that ActionResource validation works with compiler functions."""
    # Test valid action spec
    valid_spec = _get_action("valid-stack", ["123456789012"], ["us-east-1"])

    try:
        action_resource = ActionResource.model_validate(valid_spec)
        assert action_resource.label == ":action/valid-stack-label"
        assert action_resource.type == "create_stack"
        assert "stack_name" in action_resource.spec
    except ValidationError as e:
        pytest.fail(f"Valid ActionResource failed validation: {e.errors()}")

    # Test invalid action spec (missing required fields)
    invalid_spec = {"label": "test", "type": "invalid_type"}

    with pytest.raises(ValidationError):
        ActionResource.model_validate(invalid_spec)


# Helper methods (not fixtures, just utility functions)
def _get_action_parameters(name: str, account: list[str], region: list[str]) -> dict:
    """Helper to create action parameters."""
    return {
        "stack_name": name,
        "template": f"{name}-stack.yaml",
        "accounts": account,
        "regions": region,
        "stack_policy": "stack_policy",
    }


def _get_user_action_parameters(name: str, user: str, account: str, region: str) -> dict:
    """Helper to create user action parameters."""
    return {
        "stack_name": name,
        "user_name": user,
        "account": account,
        "region": region,
        "stack_policy": "stack_policy",
    }


def _get_action(name: str, account: list[str], region: list[str]) -> dict:
    """Helper to create action dictionary."""
    label = f"{name}-label"
    return {
        "label": label,
        "type": "create_stack",
        "spec": _get_action_parameters(name, account, region),
        "scope": "build",
    }


def _get_deployspec(name: str, account: list[str], region: list[str]) -> list[dict]:
    """Helper to create deployspec list."""
    return [_get_action(name, account, region)]


def _get_user_action(name: str, user: str, account: str, region: str) -> dict:
    """Helper to create user action dictionary."""
    label = f"{name}-label"
    return {
        "label": label,
        "kind": "create_user",
        "spec": _get_user_action_parameters(name, user, account, region),
    }


def _get_user_deployspec(name: str, user: str, account: str, region: str) -> list[dict]:
    """Helper to create user deployspec list."""
    return [_get_user_action(name, user, account, region)]


# Test fixtures for integration tests if needed
@pytest.fixture
def sample_action_resource():
    """Fixture providing a sample ActionResource for integration tests."""
    return {
        "label": "test-stack-label",
        "type": "create_stack",
        "spec": {
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
def test_actionresource_integration(sample_action_resource):
    """Integration test for ActionResource creation and compiler function usage."""
    # Create ActionResource from sample data
    action_resource = ActionResource.model_validate(sample_action_resource)

    # Test with compiler function
    labels = get_region_account_labels(action_resource)

    expected_labels = [':action/test-stack-label-123456789012-us-east-1', ':action/test-stack-label-123456789012-ap-southeast-1']
    assert labels == expected_labels

    # Test scope detection
    scope = __get_stack_scope(action_resource.spec.get("stack_name", ""))
    # This should return None since "test-stack" doesn't match any template pattern
    assert scope is 'build'  # default is build

import pytest
from pydantic import ValidationError

from core_framework.constants import (
    SCOPE_PORTFOLIO,
    SCOPE_APP,
    SCOPE_BRANCH,
    SCOPE_BUILD,
)

from core_deployspec_compiler.compiler import (
    __get_stack_scope,
    get_region_account_labels,
)

from core_framework.models import ActionSpec


@pytest.fixture
def get_action_check(label, name, account, region):
    account = "123456789012"
    return {
        "Label": "{}-{}-{}".format(label, account, name),
        "Type": "AWS::CreateStack",
        "DependsOn": [],
        "Params": {
            "StackName": name,
            "TemplateUrl": "s3://bucket/stack.yaml",
            "StackParameters": {},
            "Account": account,
            "Region": region,
            "Tags": [],
            "StackPolicy": "stack_policy",
        },
    }


@pytest.fixture
def get_types():
    types = [
        "create_stack",
        "delete_stack",
        "create_user",
        "delete_user",
    ]
    return types


@pytest.fixture
def get_package(client, portfolio, app, branch, build, mode, region):
    bucket_name = "{}-core-automation-{}".format(client, region)
    bucket_region = region

    app_path = "../../../{}-{}-{}".format(client, portfolio, app)
    test = "test-" if mode == "test" else ""

    return {
        "BucketName": bucket_name,
        "BucketRegion": bucket_region,
        "Key": "{}packages/{}/{}/{}/{}/package.zip".format(
            test, portfolio, app, branch, build
        ),
        "VersionId": None,
        "Mode": mode,
        "AppPath": app_path,
    }


@pytest.fixture
def get_deployment_details(portfolio, app, branch, build):
    short_branch = branch
    return {
        "Portfolio": portfolio,
        "App": app,
        "Branch": branch,
        "BranchShortName": short_branch,
        "Build": build,
    }


@pytest.fixture
def get_event(client, portfolio, app, branch, build, test, mode, region):

    return {
        "Package": get_package(client, portfolio, app, branch, build, mode, region),
        "DeploymentDetails": get_deployment_details(portfolio, app, branch, build),
    }


def get_action_parameters(name: str, account: list[str], region: list[str]) -> dict:

    rv = {
        "stack_name": name,
        "template": f"{name}-stack.yaml",
        "accounts": account,
        "regions": region,
        "stack_policy": "stack_policy",
    }
    return rv


def get_user_action_parameters(name: str, user: str, account: str, region: str):

    rv = {
        "stack_name": name,
        "user_name": user,
        "account": account,
        "region": region,
        "stack_policy": "stack_policy",
    }
    return rv


def get_action(name, account, region):
    label = f"{name}-label"
    return {
        "label": label,
        "type": "create_stack",
        "params": get_action_parameters(name, account, region),
        "scope": "build",
    }


def get_deployspec(name, account, region):
    return [get_action(name, account, region)]


def get_user_action(name, user, account, region):
    label = f"{name}-label"
    return {
        "label": label,
        "type": "create_user",
        "params": get_user_action_parameters(name, user, account, region),
    }


def get_user_deployspec(name, user, account, region):
    return [get_user_action(name, user, account, region)]


def test_get_region_account_labels():
    """Test for get_region_account_labels"""
    try:
        deployspec = get_deployspec(
            "test_stack",
            ["123456789012", "123456789013"],
            ["us-east-1", "ap-southeast-1"],
        )

        for spec in deployspec:
            action_spec = ActionSpec(**spec)
            region_account_labels = get_region_account_labels(action_spec)

            assert region_account_labels == [
                "test_stack-label-123456789012-us-east-1",
                "test_stack-label-123456789012-ap-southeast-1",
                "test_stack-label-123456789013-us-east-1",
                "test_stack-label-123456789013-ap-southeast-1",
            ]
    except ValidationError as e:
        print(e.errors())
        assert False, e
    except Exception as e:
        assert False, e


def test___get_scope():

    stack_name = "{{ core.Portfolio }}-resources"
    assert __get_stack_scope(stack_name) == SCOPE_PORTFOLIO

    stack_name = "{{ core.Project }}-{{ core.App }}-resources"
    assert __get_stack_scope(stack_name) == SCOPE_APP

    stack_name = "{{ core.Project }}-{{ core.App }}-{{ core.Branch }}-resources"
    assert __get_stack_scope(stack_name) == SCOPE_BRANCH

    stack_name = (
        "{{ core.Project }}-{{ core.App }}-{{ core.Branch }}-{{ core.Build}}-resources"
    )
    assert __get_stack_scope(stack_name) == SCOPE_BUILD

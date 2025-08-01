import pytest
import os
import core_framework as util
from core_framework.models import TaskPayload, DeploymentDetails, DeploySpec, ActionSpec
from core_deployspec.compiler import (
    generate_action_command,
    get_spec_label_map,
    get_region_account_labels,
    get_accounts_regions,
    compile_deployspec,
)


@pytest.fixture
def task_payload():
    return TaskPayload(
        Task="deploy",
        deployment_details=DeploymentDetails(
            Client="test_client",
            Environment="test_env",
            Portfolio="test_portfolio",
            App="test_app",
            Branch="test_branch",
            Build="test_build",
        ),
    )


@pytest.fixture
def deployspec() -> DeploySpec:

    base_dir = os.path.dirname(os.path.abspath(__file__))
    deployspec_filename = os.path.join(base_dir, "deployspec.yaml")
    if not os.path.exists(deployspec_filename):
        raise FileNotFoundError(f"Deployspec file not found: {deployspec_filename}")
    with open(deployspec_filename, "r") as f:
        data = util.read_yaml(f)
    return DeploySpec(**{"Actions": data})


def test_get_region_account_labels(deployspec: DeploySpec):

    label_map = get_spec_label_map(deployspec.actions)

    assert len(label_map) == 3

    assert "test1-create-user" in label_map


def test_load_and_validate_action(task_payload, deployspec: DeploySpec):

    # Load the action from the deployspec file

    label_map = get_spec_label_map(deployspec)

    assert len(label_map) == 3, "Label Map should contain 3 keys"

    action_spec = deployspec.actions[1]

    accounts, regions = get_accounts_regions(action_spec)

    assert len(accounts) == 3, "The test action should contain 3 accounts for the target"
    assert len(regions) == 3, "The test action should contain 3 regions for the target"

    account = accounts[1]
    region = regions[1]

    # Generate the single action for the account/region
    execute_action = generate_action_command(task_payload, action_spec, label_map, account, region)

    # Check if the action is loaded correctly
    assert execute_action.kind == "AWS::DeleteUser"
    assert execute_action.params["UserNames"] == ["bob"]
    assert execute_action.params["Account"] == "123456789013"


def test_generatge_create_stack(task_payload, deployspec: DeploySpec):

    label_map = get_spec_label_map(deployspec)

    assert len(label_map) == 3, "Label Map should contain 3 keys"

    ## Get the create_stack action
    action_spec = deployspec.actions[2]

    accounts, regions = get_accounts_regions(action_spec)

    assert len(accounts) == 3, "The test action should contain 3 accounts for the target"
    assert len(regions) == 3, "The test action should contain 3 regions for the target"

    account = accounts[1]
    region = regions[1]

    # Generate the single action for the account/region
    execute_action = generate_action_command(task_payload, action_spec, label_map, account, region)

    assert execute_action is not None, "Should have an create_stack action"
    assert execute_action.kind == "AWS::CreateStack", "Should be a create_stack action"
    assert execute_action.params is not None

    params = execute_action.params

    # make sure the path of our template was fixed
    assert (
        "test_client-core-automation-ap-southeast-1\\artefacts\\test_portfolio\\test_app\\test-branch\\test_build\\"
        in params["TemplateUrl"]
    )

    # Did the translation work?
    assert params["StackParameters"]["BucketName"] == "{{ 'portfolio/name' | lookup }}"

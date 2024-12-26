from pydantic import ValidationError
import pytest

from core_deployspec_compiler.handler import handler

from core_framework.models import DeploymentDetails, PackageDetails, TaskPayload


@pytest.fixture
def runtime_arguments():

    # These typically come from environment variables or command line arguments

    return {
        "client": "my-client",
        "portfolio": "my-portfolio",
        "app": "my-app",
        "branch": "my-branch",
        "build": "my-build",
        "mode": "local",
    }


@pytest.fixture
def deployment_details(runtime_arguments: dict):

    deployment_details = DeploymentDetails.from_arguments(**runtime_arguments)

    return deployment_details


@pytest.fixture
def task_payload(runtime_arguments: dict, deployment_details: DeploymentDetails):

    task_payload = TaskPayload.from_arguments(
        deployment_details=deployment_details, **runtime_arguments
    )

    return task_payload


@pytest.fixture
def package_details(runtime_arguments: dict, deployment_details: DeploymentDetails):

    package_details = PackageDetails.from_arguments(
        **{"deployment_details": deployment_details, **runtime_arguments}
    )

    return package_details


def test_deployspec_compiler(task_payload: TaskPayload):

    try:
        assert task_payload is not None

        event = task_payload.model_dump()

        result = handler(event, None)

        assert result is not None

    except ValidationError as e:
        print(e.errors())
        pytest.fail(f"Test failed: {e}")
    except Exception as e:
        pytest.fail(f"Test failed: {e}")

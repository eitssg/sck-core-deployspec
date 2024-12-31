import os
from pydantic import ValidationError
import pytest

import core_framework as util
from core_deployspec.handler import handler

from core_framework.models import DeploymentDetails, PackageDetails, TaskPayload
from core_framework.constants import V_PACKAGE_ZIP, V_DEPLOYSPEC_FILE_YAML


@pytest.fixture
def runtime_arguments():

    # These typically come from environment variables or command line arguments
    client = util.get_client()
    data_dir = util.get_storage_volume()

    # These typically come from environment variables or command line arguments
    # The bucket name is the standard core-automation folder name
    bucket_name = util.get_bucket_name()

    artefact_path = os.path.join(
        data_dir,
        bucket_name,
        "packages",
        "my-portfolio",
        "my-app",
        "my-branch",
        "my-build",
    )

    os.makedirs(artefact_path, exist_ok=True)

    zipfilename = os.path.join(artefact_path, V_PACKAGE_ZIP)

    dirname = os.path.dirname(os.path.realpath(__file__))

    # create or update our test package zip with our test deployspec.yaml file.
    os.system(
        f"cd {dirname} && 7z a {zipfilename} {V_DEPLOYSPEC_FILE_YAML} template.yaml"
    )

    return {
        "client": client,
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

        assert "Artefact" in result

        assert result["Artefact"]["Scope"] == "deployspec"

        assert result["Artefact"]["Key"] == os.path.join(
            "artefacts",
            "my-portfolio",
            "my-app",
            "my-branch",
            "my-build",
            "deploy.actions",
        )

    except ValidationError as e:
        print(e.errors())
        pytest.fail(f"Test failed: {e}")
    except Exception as e:
        pytest.fail(f"Test failed: {e}")

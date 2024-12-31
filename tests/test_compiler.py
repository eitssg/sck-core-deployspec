from unittest.mock import patch
from pydantic import ValidationError
import pytest
import os

import core_framework as util
from core_framework.models import (
    TaskPayload,
    PackageDetails,
    ActionDefinition,
    ActionSpec,
    DeploySpec,
)
from core_framework.constants import (
    V_LOCAL,
    V_PACKAGE_ZIP,
    V_DEPLOYSPEC_FILE_YAML,
)
from core_framework.magic import MagicS3Client

from core_deployspec.compiler import (
    process_package_local,
    process_package_s3,
    compile_deployspec,
)


@pytest.fixture
def runtime_arguments():

    client = util.get_client()
    data_dir = util.get_storage_volume()

    # These typically come from environment variables or command line arguments
    # The bucket name is the standard core-automation folder name
    bucket_name = util.get_bucket_name()

    artefact_path = os.path.join(data_dir, bucket_name)

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
        "scope": "portfolio",
        "environment": "dev",
        "data_center": "us-east-1",
        "package_key": V_PACKAGE_ZIP,
    }


def test_process_package_local(runtime_arguments):

    try:
        # get the current script folder
        package_details = PackageDetails.from_arguments(**runtime_arguments)

        # Should return a DeploySpec object from the package.zip
        result = process_package_local(package_details)

        assert result is not None

        assert isinstance(result, DeploySpec)

        assert isinstance(result.action_specs, list)

        assert isinstance(result.action_specs[0], ActionSpec)

        assert len(result.action_specs) == 6

    except Exception as e:
        print(e)
        assert False, e


def get_s3_resource(bucket_region):
    """For the Mock"""
    return MagicS3Client(Region=bucket_region)


def test_process_package_zip(runtime_arguments):

    # Our test zip file has two files in it,

    # 1. template.yaml
    # 2. deployspec.yaml

    try:
        """Because we created "mocks", it will behave much like 'local'"""
        with patch("core_helper.aws.s3_resource", get_s3_resource):

            # when the mock_s3_resource.Bukcket() mthod is called, return mock_bucket intance

            package_details = PackageDetails.from_arguments(**runtime_arguments)

            assert package_details.Mode == V_LOCAL

            # Call the patched download_fileobj method
            deployspec = process_package_s3(package_details, upload_prefix="artefacts")

            assert deployspec is not None

            assert len(deployspec.action_specs) == 6

            # Inside of the zip file is a template.yaml file.  check to see if it was uploaded
            assert package_details.Key == "package.zip"

    except Exception as e:
        print(e)
        assert False, e


def test_compile_deployspec(runtime_arguments):

    try:
        task_payload = TaskPayload.from_arguments(**runtime_arguments)

        assert task_payload is not None

        deployspec = process_package_local(task_payload.Package)

        assert deployspec is not None

        assert task_payload.Package.DeploySpec is not None

        assert deployspec == task_payload.Package.DeploySpec

        # The result is DeploySpec.actonis = list[ActioSpec] list translated
        # into list[ActionDefinition] object.
        actions_list = compile_deployspec(task_payload, deployspec)

        assert actions_list is not None

        assert len(actions_list) == 6

        assert isinstance(actions_list[0], ActionDefinition)

        assert len(deployspec.action_specs) == len(actions_list)

    except ValidationError as e:
        print("Validation error occurred:")
        for error in e.errors():
            print(f"Field: {error['loc']}, Error: {error['msg']}")
        assert False, e
    except Exception as e:
        print(e)
        assert False, e

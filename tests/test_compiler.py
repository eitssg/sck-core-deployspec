from unittest.mock import patch
from pydantic import ValidationError
import pytest
import os

from core_framework.models import TaskPayload, PackageDetails, ActionDefinition
from core_framework.constants import V_LOCAL

from core_deployspec_compiler.compiler import (
    process_package_local,
    process_package_s3,
    compile_deployspec,
)


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
        "scope": "portfolio",
        "environment": "dev",
        "data_center": "us-east-1",
    }


def test_process_package_local(runtime_arguments):

    try:
        # get the current script folder
        package_details = PackageDetails(**runtime_arguments)
        package_details.AppPath = os.path.dirname(os.path.realpath(__file__))

        result = process_package_local(package_details)

        print(result)

        assert result is not None

        assert len(result.action_specs) == 6

    except Exception as e:
        print(e)
        assert False, e


class MagicBucket:

    key = None

    def __init__(self, key):
        self.key = key

    def download_fileobj(self, key, fileobj):
        with open(key, "rb") as file:
            fileobj.write(file.read())
        fileobj.seek(0)

    def put_object(self, **kwargs):
        self.key = kwargs["Key"]
        pass


testingBucket: MagicBucket | None = None


class S3Resource:

    name: str | None = None
    region: str | None = None

    def __init__(self, region):
        self.region = region

    def Bucket(self, name):
        global testingBucket

        self.name = name
        testingBucket = MagicBucket(name)
        return testingBucket


def get_s3_resource(name):

    return S3Resource(name)


def test_process_package_zip():

    # Our test zip file has two files in it,

    # 1. template.yaml
    # 2. deployspec.yaml

    try:
        with patch("aws.s3_resource", get_s3_resource):

            # when the mock_s3_resource.Bukcket() mthod is called, return mock_bucket intance

            app_path = os.path.dirname(os.path.realpath(__file__))
            key = os.path.join(app_path, "deployspec.zip")
            package_details = PackageDetails(AppPath=app_path, Key=key)

            assert package_details.Mode == V_LOCAL

            # Call the patched download_fileobj method
            deployspec = process_package_s3(package_details, upload_prefix="artefacts")

            assert deployspec is not None

            # assert deployspec is not None
            print(deployspec)

            assert len(deployspec.action_specs) == 6

            # Inside of the zip file is a template.yaml file.  check to see if it was uploaded
            assert testingBucket.key == "artefacts/template.yaml"

    except Exception as e:
        print(e)
        assert False, e


def test_compile_deployspec(runtime_arguments):

    try:
        task_payload = TaskPayload.from_arguments(**runtime_arguments)

        assert task_payload is not None

        task_payload.Package.AppPath = os.path.dirname(os.path.realpath(__file__))

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

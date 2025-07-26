import pytest
import re
import os
import tempfile
import shutil
import zipfile
from pathlib import Path

from pydantic import ValidationError

import core_framework as util

from core_db.facter import get_facts

from core_framework.models import TaskPayload, PackageDetails
from core_framework.constants import (
    V_PACKAGE_ZIP,
    V_DEPLOYSPEC_FILE_YAML,
    V_PLANSPEC_FILE_YAML,
    V_APPLYSPEC_FILE_YAML,
    V_TEARDOWNSPEC_FILE_YAML,
)

from core_helper.magic import MagicS3Client

from .data_for_testing import initialize

# Import the handler, not the compiler module
from core_deployspec.handler import handler as deployspec_handler


@pytest.fixture(scope="module")
def arguments():
    """Fixture providing command line arguments simulation."""
    client = util.get_client()  # from the --client parameter
    task = "compile"  # from the "command" positional parameter
    portfolio = "my-portfolio"  # from the -p, --portfolio parameter
    app = "my-app"  # from the -a, --app parameter
    branch = "my-branch"  # from the -b --branch parameter
    build = "dp-build"  # from the -i, --build parameter
    automation_type = "deployspec"  # from the --automation-type parameter

    # commandline example:
    # core --client my-client compile -p my-portfolio -a my-app -b my-branch -i dp-build --automation-type deployspec

    state = {
        "client": client,
        "task": task,
        "portfolio": portfolio,
        "app": app,
        "branch": branch,
        "build": build,
        "automation_type": automation_type,
    }

    return state


@pytest.fixture(scope="module")
def package_package():
    """Fixture creating a test package zip file."""
    # Typical lifecycle is: -> package -> upload -> compile -> deploy -> teardown
    # This is the "package" step. Create the zip file

    dirname = Path(__file__).parent
    package_path = dirname / V_PACKAGE_ZIP

    # Files to include in package
    files_to_package = [
        V_DEPLOYSPEC_FILE_YAML,
        V_PLANSPEC_FILE_YAML,
        V_APPLYSPEC_FILE_YAML,
        V_TEARDOWNSPEC_FILE_YAML,
        "template.yaml",
    ]

    # Create zip file using Python's zipfile module (cross-platform)
    try:
        with zipfile.ZipFile(package_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file_name in files_to_package:
                file_path = dirname / file_name
                if file_path.exists():
                    zipf.write(file_path, file_name)
                else:
                    pytest.skip(f"Required test file {file_name} not found in {dirname}")

        yield str(package_path)

    finally:
        # Cleanup: Remove the created zip file
        if package_path.exists():
            package_path.unlink()


@pytest.fixture(scope="module")
def task_payload(arguments: dict) -> TaskPayload:
    """Fixture creating TaskPayload from arguments."""
    assert isinstance(arguments, dict)

    # Typical lifecycle is: -> package -> upload -> compile -> deploy -> teardown
    # This creates the task payload for testing

    task_payload = TaskPayload.from_arguments(**arguments)
    return task_payload


@pytest.fixture(scope="module")
def upload_package(task_payload: TaskPayload, package_package: str):
    """Fixture uploading package to S3 and providing cleanup."""
    assert isinstance(task_payload, TaskPayload)
    assert isinstance(package_package, str)

    # Typical lifecycle is: -> package -> upload -> compile -> deploy | plan -> deploy | apply -> teardown
    # This is the "upload" step

    package_details = task_payload.package
    bucket = MagicS3Client(Region=package_details.bucket_region).Bucket(package_details.bucket_name)

    try:
        # Upload package.zip (should be small - a few MB is ok, but 100MB is not)
        with open(package_package, "rb") as f:
            bucket.put_object(Key=package_details.key, Body=f.read())

        yield package_details

    except Exception as e:
        pytest.fail(f"Failed to upload package: {e}")

    finally:
        # Cleanup: Remove uploaded S3 object
        try:
            bucket.delete_object(Key=package_details.key)
        except Exception as cleanup_error:
            print(f"Warning: Failed to cleanup S3 object {package_details.key}: {cleanup_error}")


@pytest.fixture(scope="module")
def facts(task_payload: TaskPayload, arguments: dict):
    """Fixture providing deployment facts."""
    cf, zf, pf, af = initialize(arguments)

    deployment_details = task_payload.deployment_details

    facts = get_facts(deployment_details)

    assert facts is not None
    assert facts["Client"] == cf.Client
    assert facts["Portfolio"] == pf.Portfolio
    assert facts["Zone"] == zf.Zone
    assert facts["AppRegex"] == af.AppRegex
    assert re.match(facts["AppRegex"], deployment_details.get_identity())

    return facts


class TestDeployspecHandler:
    """Test class for deployspec handler functionality."""

    def test_deployspec_handler_compilation_and_execution(
        self,
        task_payload: TaskPayload,
        upload_package: PackageDetails,
        facts: dict,
        arguments: dict,
    ):
        """Test the complete deployspec handler workflow."""
        # Typical lifecycle is: -> package -> upload -> compile -> deploy -> teardown
        # This tests the "compile" and "execute" steps handled by the handler

        try:
            assert isinstance(facts, dict)
            assert isinstance(arguments, dict)
            assert isinstance(task_payload, TaskPayload)
            assert isinstance(upload_package, PackageDetails)

            # Call the handler (not the compiler module)
            result = deployspec_handler(task_payload.model_dump(), None)

            assert result is not None, "Handler should return a result"
            assert "Response" in result, "Result should contain 'Response' key"

            response = result["Response"]

            # Test compilation summary
            assert "compilation_summary" in response, "Response should contain compilation summary"
            compilation_summary = response["compilation_summary"]

            assert "specs_compiled" in compilation_summary
            assert "total_actions_generated" in compilation_summary
            assert len(compilation_summary["specs_compiled"]) > 0, "Should have compiled at least one spec"

            # Test execution results
            assert "execution_results" in response, "Response should contain execution results"
            execution_results = response["execution_results"]

            assert isinstance(execution_results, list), "Execution results should be a list"
            assert len(execution_results) > 0, "Should have execution results"

            # Test task payload creation
            assert "task_payloads_created" in response, "Response should contain created task payloads"
            task_payloads_created = response["task_payloads_created"]

            assert isinstance(task_payloads_created, list), "Task payloads should be a list"
            assert len(task_payloads_created) > 0, "Should have created task payloads"

            # Verify task types
            expected_tasks = ["deploy", "teardown", "plan", "apply"]
            created_tasks = [tp["task"] for tp in task_payloads_created]

            for task in created_tasks:
                assert task in expected_tasks, f"Task '{task}' should be one of {expected_tasks}"

            # Test status
            assert "status" in response, "Response should contain status"
            status = response["status"]
            assert status in ["EXECUTION_COMPLETE", "EXECUTION_FAILED"], f"Status should be valid, got: {status}"

            print(f"✅ Handler test passed - Compiled {len(compilation_summary['specs_compiled'])} specs")

        except ValidationError as e:
            print(f"Validation errors: {e.errors()}")
            pytest.fail(f"Validation failed: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            pytest.fail(f"Test failed: {e}")

    def test_deployspec_handler_compilation_only(
        self,
        task_payload: TaskPayload,
        upload_package: PackageDetails,
    ):
        """Test just the compilation part of the handler."""
        try:
            # Call handler
            result = deployspec_handler(task_payload.model_dump(), None)

            assert result is not None
            response = result["Response"]

            # Focus on compilation results
            compilation_summary = response["compilation_summary"]

            assert "specs_found" in compilation_summary
            assert "specs_compiled" in compilation_summary
            assert "total_actions_generated" in compilation_summary

            specs_found = compilation_summary["specs_found"]
            specs_compiled = compilation_summary["specs_compiled"]

            # All found specs should be compiled (assuming no errors)
            assert len(specs_compiled) <= len(specs_found), "Compiled specs should not exceed found specs"

            # Should have some actions generated
            assert compilation_summary["total_actions_generated"] > 0, "Should generate some actions"

        except Exception as e:
            pytest.fail(f"Compilation test failed: {e}")

    def test_deployspec_handler_error_handling(self):
        """Test handler error handling with invalid input."""
        # Test with invalid task payload
        invalid_payload = {"invalid": "data"}

        with pytest.raises(Exception):
            deployspec_handler(invalid_payload, None)

    @pytest.mark.parametrize("missing_field", ["deployment_details", "Package"])
    def test_deployspec_handler_missing_required_fields(self, task_payload: TaskPayload, missing_field):
        """Test handler behavior with missing required fields."""
        payload_dict = task_payload.model_dump()

        # Remove required field
        if missing_field in payload_dict:
            del payload_dict[missing_field]

        with pytest.raises((ValidationError, KeyError, AttributeError)):
            deployspec_handler(payload_dict, None)


# Legacy test function for backward compatibility
def test_deployspec_compiler(
    task_payload: TaskPayload,
    upload_package: PackageDetails,
    facts: dict,
    arguments: dict,
):
    """Legacy test function - use TestDeployspecHandler class instead."""
    # Call the new test class method
    test_instance = TestDeployspecHandler()
    test_instance.test_deployspec_handler_compilation_and_execution(task_payload, upload_package, facts, arguments)

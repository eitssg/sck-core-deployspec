from typing import Any
import os

import core_logging as log

import core_helper.aws as aws

from core_db.facter import get_facts

from core_framework.constants import (
    TP_DEPLOYMENT_DETAILS,
    V_EMPTY,
    V_LOCAL,
)
from core_framework.status import COMPILE_FAILED, COMPILE_COMPLETE, COMPILE_IN_PROGRESS

from .compiler import (
    apply_state,
    compile_deployspec,
    process_package_local,
    process_package_s3,
    to_yaml,
    upload_actions,
)

from core_framework.models import DeploySpec, TaskPayload, ActionDefinition
from core_framework.magic import MagicS3Client


def load_deployspec(task_payload: TaskPayload) -> DeploySpec:

    deployment_details = task_payload.DeploymentDetails
    package_details = task_payload.Package

    if package_details.Mode == V_LOCAL:
        upload_prefix = deployment_details.get_artefacts_key(s3=False)
        deployspec = process_package_local(package_details, upload_prefix)
    else:
        upload_prefix = deployment_details.get_artefacts_key(s3=True)
        deployspec = process_package_s3(package_details, upload_prefix)

    return deployspec


def upload_actions_output(
    task_payload: TaskPayload, actions_output: str
) -> tuple[str, str]:
    """
    Upload the compiled actions to the target defined in deployment details

    Args:
        task_payload (dict): The task payload contianng the deployment details and package details
        actions_output (str): The compiled actions in yaml or JSON format

    Returns:
        tuple: returns the action key and version
    """
    deployment_details = task_payload.DeploymentDetails
    package_details = task_payload.Package
    bucket_name = package_details.BucketName
    bucket_region = package_details.BucketRegion

    upload_prefix = V_EMPTY

    # Process the package and retrieve the deployspec

    if package_details.Mode == V_LOCAL:
        # Upload to Local

        upload_prefix = deployment_details.get_artefacts_key(s3=False)

        log.debug("upload_prefix={}", upload_prefix)

        # Download file from Local
        local = MagicS3Client(Region=bucket_region, AppPath=package_details.AppPath)
        bucket = local.Bucket(bucket_name)

        # Normal flow means upload compiled actions to Local.
        actions_key, actions_version = upload_actions(
            bucket, upload_prefix, actions_output, os.path.sep
        )

    else:
        # Upload to S3

        upload_prefix = deployment_details.get_artefacts_key(s3=True)

        log.debug("upload_prefix={}", upload_prefix)

        # Download file from S3
        s3 = aws.s3_resource(bucket_region)
        bucket = s3.Bucket(bucket_name)

        # Normal flow means upload compiled actions to S3.
        actions_key, actions_version = upload_actions(
            bucket, upload_prefix, actions_output
        )

    # Mutate task_payload with the actions version that was saved
    task_payload.Actions.Key = actions_key
    task_payload.Actions.VersionId = actions_version

    return actions_key, actions_version


def handler(event: dict, context: Any | None) -> dict:
    """
    Lambda handler function.

    The event object MUST be a TaskPayload object.

        ```python
        # Creating from commandline arguments
        task_payload = TaskPayload.from_arguments(**kwargs)

        # Creating from a task_payload dictionary
        task_paylpad = TaskPayload(**event)

        # Creating from a task_payload dictionary
        event = task_payload.model_dump()
        ```

    The lambda invokder should be called with a TaskPayload dictionary object.

    Args:
        event (dict): The event object / a task payload dictionary
        context (Any, optional): The context object

    """

    log.debug("event", details=event)

    try:
        task_payload = TaskPayload(**event)

        deployment_details = task_payload.DeploymentDetails

        log.setup(deployment_details.get_identity())
        log.status(
            COMPILE_IN_PROGRESS,
            "Deployspec compilation started",
            details={"Scope": "deployspec"},
        )
        deployspec = load_deployspec(task_payload)

        # Compile the deployspec into actions
        actions: list[ActionDefinition] = compile_deployspec(task_payload, deployspec)

        log.debug("Finalizing Templates.  Jinja2 templating.")

        # Get the Jinja2 context for variable replacment if Jinja is in the the text.
        state = get_context(deployment_details)

        dictlist = [a.model_dump(exclude_none=True) for a in actions]

        # Apply the context and finalize output
        actions_output = to_yaml(dictlist)
        actions_output = apply_state(actions_output, state)

        # Upload the compiled actions to the target defined specified by the deployment details
        key, version = upload_actions_output(task_payload, actions_output)

        artefact_info = {"Scope": "deployspec", "Key": key, "Version": version}

        log.status(
            COMPILE_COMPLETE,
            "Deployspec compilation successful",
            details=artefact_info,
        )

        return {
            TP_DEPLOYMENT_DETAILS: deployment_details.model_dump(),
            "Artefact": artefact_info,
        }

    except Exception as e:
        log.status(
            COMPILE_FAILED,
            "Deployspec compilation failed",
            details={"Scope": "deployspec", "Error": str(e)},
        )
        raise


def get_context(task_payload: TaskPayload) -> dict:
    """
    Get the context for the Jinja2 templating

    Args:
        task_payload (TaskPayload): The task payload object

    Returns:
        dict: The context for the Jinja2 templating
    """
    deployment_details = task_payload.DeploymentDetails

    # Get the facts for the deployment
    facts = get_facts(deployment_details)

    return {"context": facts}

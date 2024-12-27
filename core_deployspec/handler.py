from typing import Any
import os
import core_logging as log

import core_helper.aws as aws

import core_framework as util
from core_framework.constants import (
    TP_DEPLOYMENT_DETAILS,
    SCOPE_PORTFOLIO,
    SCOPE_APP,
    SCOPE_BRANCH,
    SCOPE_BUILD,
    V_EMPTY,
    V_LOCAL,
)
from core_framework.status import COMPILE_FAILED, COMPILE_COMPLETE, COMPILE_IN_PROGRESS

from .compiler import (
    apply_state,
    compile_deployspec,
    get_template_url,
    process_package_local,
    process_package_s3,
    to_yaml,
    upload_actions,
    LocalBucket,
)

from core_framework.models import DeploySpec, TaskPayload, ActionDefinition


def load_deployspec(task_payload: TaskPayload) -> DeploySpec:

    deployment_details = task_payload.DeploymentDetails
    package_details = task_payload.Package

    if package_details.Mode == V_LOCAL:
        upload_prefix = util.get_artefacts_path(deployment_details, scope=SCOPE_BUILD)
        deployspec = process_package_local(package_details, upload_prefix)
    else:
        upload_prefix = util.get_artefact_key(
            deployment_details, scope=task_payload.DeploymentDetails.Scope
        )
        deployspec = process_package_s3(package_details, upload_prefix)

    return deployspec


def generate_state(task_payload: TaskPayload, scope: str = SCOPE_BUILD) -> dict:
    """
    This state is used so that you can use Jinja2 templating in your deployspec

    Args:
        task_payload (dict): _description_

    Returns:
        dict: _description_
    """

    deployment_details = task_payload.DeploymentDetails
    package_details = task_payload.Package
    bucket_name = package_details.BucketName
    bucket_region = package_details.BucketRegion

    # Dump to string and find/replace the state. We do this as late as possible because some logic inspects the
    # contents for stuff like stack_name BEFORE it's replaced.
    state: dict = {
        "core": {
            "ArtifactBucketName": bucket_name,
            "ArtifactBucketRegion": bucket_region,
            "ArtifactKeyPrefix": util.get_artefact_key(
                deployment_details, scope=SCOPE_BUILD
            ),
            "ArtifactKeyBuildPrefix": util.get_artefact_key(
                deployment_details, scope=SCOPE_BUILD
            ),
            "ArtifactKeyBranchPrefix": util.get_artefact_key(
                deployment_details, scope=SCOPE_BRANCH
            ),
            "ArtifactKeyAppPrefix": util.get_artefact_key(
                deployment_details, scope=SCOPE_APP
            ),
            "ArtifactKeyPortfolioPrefix": util.get_artefact_key(
                deployment_details, scope=SCOPE_PORTFOLIO
            ),
            "ArtifactBaseUrl": get_template_url(
                bucket_name, bucket_region, deployment_details, None, scope
            ),
            **deployment_details.model_dump(),
        }
    }

    return state


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

    if package_details.Mode == V_LOCAL:

        upload_prefix = util.get_artefacts_path(deployment_details)

        log.debug("upload_prefix={}", upload_prefix)

        bucket = LocalBucket(bucket_name, package_details.AppPath)

        actions_key, actions_version = upload_actions(
            bucket, upload_prefix, actions_output, os.path.sep
        )
    else:

        # Normal flow.
        # Process the package and retrieve the deployspec
        upload_prefix = util.get_artefact_key(deployment_details)

        log.debug("upload_prefix={}", upload_prefix)

        # Download file from S3
        s3 = aws.s3_resource(bucket_region)
        bucket = s3.Bucket(bucket_name)

        # Normal flow means upload compiled actions to S3.
        actions_key, actions_version = upload_actions(
            bucket, upload_prefix, actions_output
        )

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
        state = generate_state(task_payload)

        dictlist = [a.model_dump(exclude_none=True) for a in actions]

        # Apply the context and finalize output
        actions_output = to_yaml(dictlist)
        actions_output = apply_state(actions_output, state)

        # Upload the compiled actions to the target defined specified by the deployment details
        key, version = upload_actions_output(task_payload, actions_output)

        log.status(
            COMPILE_COMPLETE,
            "Deployspec compilation successful",
            details={"Scope": "deployspec", "Key": key, "Version": version},
        )

        return {
            TP_DEPLOYMENT_DETAILS: deployment_details.model_dump(),
            "Artefact": {
                "Scope": "deployspec",
                "Key": key,
                "Version": version,
            },
        }

    except Exception as e:
        log.status(
            COMPILE_FAILED,
            "Deployspec compilation failed",
            details={"Scope": "deployspec", "Error": str(e)},
        )
        raise

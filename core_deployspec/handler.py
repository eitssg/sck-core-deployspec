from typing import Any

import core_logging as log

from core_framework.constants import (
    TP_DEPLOYMENT_DETAILS,
    TR_RESPONSE,
)
from core_framework.status import COMPILE_FAILED, COMPILE_COMPLETE, COMPILE_IN_PROGRESS

from .compiler import (
    apply_context,
    compile_deployspec,
    load_deployspec,
    upload_actions,
    get_context,
)

from core_framework.models import TaskPayload, ActionDefinition


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

    This function returns with Task Response { "Response": "..." }

    Args:
        event (dict): The event object / a task payload dictionary
        context (Any, optional): The context object

    Returns:
        dict: The Task Response object { "Response": "..." }

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
        context = get_context(task_payload)

        actions_list = [a.model_dump(exclude_none=True) for a in actions]

        # Apply the context and finalize output.  Expect the final yaml output.
        actions_output = apply_context(actions_list, context)

        # Upload the compiled actions to the target defined specified by the deployment details
        key, version = upload_actions(task_payload, actions_output)

        artefact_info = {"Scope": "deployspec", "Key": key, "Version": version}

        log.status(
            COMPILE_COMPLETE,
            "Deployspec compilation successful",
            details=artefact_info,
        )

        return {
            TR_RESPONSE: {
                TP_DEPLOYMENT_DETAILS: deployment_details.model_dump(),
                "Artefact": artefact_info,
            }
        }

    except Exception as e:
        log.status(
            COMPILE_FAILED,
            "Deployspec compilation failed",
            details={"Scope": "deployspec", "Error": str(e)},
        )
        raise

"""
Lambda handler for deployspec compilation.

This module handles the compilation of deployment specifications into executable actions,
applies Jinja2 templating with deployment context, and coordinates execution of the
compiled actions through the core_execute module.
"""

from typing import Any

import core_logging as log
import core_framework as util

from core_framework.constants import (
    TP_DEPLOYMENT_DETAILS,
    TR_RESPONSE,
)
from core_framework.status import COMPILE_FAILED, COMPILE_COMPLETE, COMPILE_IN_PROGRESS
from core_execute.handler import invoke_execute_handler

from .compiler import (
    apply_context,
    compile_deployspec,
    load_deployspec,
    get_context,
)

# Add imports for save_actions and save_state from core_execute
from core_execute.execute import save_actions, save_state

from core_framework.models import ActionSpec, TaskPayload


def handler(event: dict, context: Any | None) -> dict:
    """
    Lambda handler function for deployspec compilation.

    Processes deployment specifications, compiles them into executable actions,
    applies Jinja2 templating with deployment context, and coordinates execution.

    :param event: Lambda event containing TaskPayload data
    :type event: dict
    :param context: Lambda context object (unused)
    :type context: Any | None
    :returns: Compilation response with summary and deployment details
    :rtype: dict
    :raises Exception: For compilation or execution failures

    Examples
    --------
    >>> event = {
    ...     "deployment_details": {...},
    ...     "package": {"bucket_name": "my-bucket", "key": "package.zip"},
    ...     "task": "deploy"
    ... }
    >>> response = handler(event, None)
    >>> # Returns: {"TaskResponse": {"status": "COMPILE_COMPLETE", ...}}
    """

    try:
        task_payload = TaskPayload(**event)
        deployment_details = task_payload.deployment_details

        log.setup(deployment_details.get_identity())
        log.debug("Task Payload: ", details=event)
        log.status(COMPILE_IN_PROGRESS, "Deployspec compilation started")

        # Get the Jinja2 context for variable replacement. A.k.a FACTS
        context_data = get_context(task_payload)

        # Read all the deployspecs from the task payload package
        specs = load_deployspec(task_payload)

        compilation_summary = {
            "specs_found": list(specs.keys()),
            "specs_compiled": [],
            "total_actions_generated": 0,
            "compilation_status": "success",
        }

        task_payloads: list[TaskPayload] = []

        log.debug("Compiling deployspecs", details=specs)

        # Compile all deployspecs in the package (deploy, teardown, plan, apply)
        for task, spec in specs.items():

            # Create a new task-specific payload by copying the original
            task_specific_payload = TaskPayload(**task_payload.model_dump())
            task_specific_payload.task = task
            task_payloads.append(
                task_specific_payload
            )  # Fixed: append the task_specific_payload

            log.debug(f"Processing task: {task}", details=spec.model_dump())

            # Compile the deployspec into actions
            actions: list[ActionSpec] = compile_deployspec(task_specific_payload, spec)

            log.debug("Finalizing Templates. Jinja2 templating.")

            # Apply the context and finalize output
            actions_output: list[ActionSpec] = apply_context(actions, context_data)

            save_actions(task_specific_payload, actions_output)

            # Save state (progressive commits)
            save_state(task_specific_payload, context_data)

            # Update compilation summary
            compilation_summary["specs_compiled"].append(task)
            compilation_summary["total_actions_generated"] += len(actions_output)

        log.status(
            COMPILE_COMPLETE,
            "Deployspec compilation successful",
            details=compilation_summary,
        )

        # Return the final response
        log.debug("Returning compilation summary", details=compilation_summary)

        return {
            TR_RESPONSE: {
                TP_DEPLOYMENT_DETAILS: deployment_details.model_dump(),
                "compilation_summary": compilation_summary,
                "task_payload": {
                    "tasks": [tp.task for tp in task_payloads],
                    "environment": task_payload.deployment_details.environment,
                },
                "status": "COMPILE_COMPLETE",
                "message": f"Successfully compiled {len(compilation_summary['specs_compiled'])} deployspec(s)",
            }
        }

    except Exception as e:
        # Enhanced error handling with context
        error_details = {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "compilation_status": "failed",
        }

        # Add context if available
        try:
            if "compilation_summary" in locals():
                error_details["specs_compiled_before_failure"] = compilation_summary[
                    "specs_compiled"
                ]
        except (
            Exception
        ) as context_error:  # Fixed: catch specific exception instead of bare except
            log.warning(f"Failed to add error context: {str(context_error)}")

        log.status(
            COMPILE_FAILED,
            "Deployspec compilation failed",
            details=error_details,
        )

        return {
            TR_RESPONSE: {
                "status": "COMPILE_FAILED",
                "error": error_details,
                "message": f"Deployspec compilation failed: {str(e)}",
            }
        }

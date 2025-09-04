"""Description: Compile a deployspec package into actions and templates.

- Extracts package files to a location in S3
- Parses and compiles the package deployspec (deployspec.yml)
- Uploads actions to S3
"""

from typing import Any

import io
import os
import re
import zipfile as zip
import tempfile

import core_logging as log

import core_framework as util
from copy import deepcopy

from core_execute.actionlib.factory import ActionFactory
from core_renderer import Jinja2Renderer

from core_framework.constants import (
    SCOPE_PORTFOLIO,
    SCOPE_APP,
    SCOPE_BRANCH,
    SCOPE_BUILD,
    DD_PORTFOLIO,
    DD_APP,
    DD_BRANCH,
    DD_BUILD,
    TAG_PORTFOLIO,
    TAG_APP,
    TAG_BRANCH,
    TAG_BUILD,
    V_PACKAGE_ZIP,
    V_DEPLOYSPEC_FILE_YAML,
    V_DEPLOYSPEC_FILE_JSON,
    V_PLANSPEC_FILE_YAML,
    V_PLANSPEC_FILE_JSON,
    V_APPLYSPEC_FILE_YAML,
    V_APPLYSPEC_FILE_JSON,
    V_TEARDOWNSPEC_FILE_YAML,
    V_TEARDOWNSPEC_FILE_JSON,
    TASK_PLAN,
    TASK_DEPLOY,
    TASK_APPLY,
    TASK_TEARDOWN,
)

from core_db.facter import get_facts

from core_framework.models import (
    ActionResource,
    TaskPayload,
    DeploySpec,
    ActionResource,
    DeploymentDetails,
)

from core_helper.magic import MagicS3Client, SeekableStreamWrapper


SpecLabelMapType = dict[str, list[str]]

CONTEXT_ROOT = "core"

spec_mapping = {
    V_DEPLOYSPEC_FILE_YAML: (util.read_yaml, TASK_DEPLOY),
    V_DEPLOYSPEC_FILE_JSON: (util.read_json, TASK_DEPLOY),
    V_PLANSPEC_FILE_YAML: (util.read_yaml, TASK_PLAN),
    V_PLANSPEC_FILE_JSON: (util.read_json, TASK_PLAN),
    V_APPLYSPEC_FILE_YAML: (util.read_yaml, TASK_APPLY),
    V_APPLYSPEC_FILE_JSON: (util.read_json, TASK_APPLY),
    V_TEARDOWNSPEC_FILE_YAML: (util.read_yaml, TASK_TEARDOWN),
    V_TEARDOWNSPEC_FILE_JSON: (util.read_json, TASK_TEARDOWN),
}


def load_deployspec(task_payload: TaskPayload) -> dict[str, DeploySpec]:

    try:

        package_key = task_payload.package.key
        if not package_key:
            raise ValueError("Package key is required to load deployspec")

        if package_key.lower().endswith(".zip"):
            return __load_deployspec_zip(task_payload)
        else:
            return __load_deployspec_file(task_payload)

    except Exception as e:
        log.error("Error loading deployspec: {}", str(e))
        return None


def __load_deployspec_file(task_payload: TaskPayload) -> dict[str, DeploySpec]:
    """
    Process package for single file.

    Package details will contain the location of the deployspec file.
    This routine will read the file and process it.

    It will return the deployment specifications as an index of tasks.

    :param task_payload: The task payload containing package and deployment details
    :type task_payload: TaskPayload
    :returns: Dictionary of deployment specifications keyed by task type
    :rtype: dict[str, DeploySpec]
    :raises ValueError: If package key is required but not provided

    Examples
    --------
    >>> task_payload = TaskPayload(
    ...     package=PackageDetails(
    ...         bucket_name="my-bucket",
    ...         bucket_region="us-east-1",
    ...         key="deployments/package.zip"
    ...     )
    ... )
    >>> specs = load_deployspec(task_payload)
    >>> # Returns: {"deploy": DeploySpec(...), "plan": DeploySpec(...)}
    """
    package_key = task_payload.package.key

    if package_key.lower().endswith(".yaml") or package_key.lower().endswith(".yml"):
        mimetype = "application/yaml"

    elif package_key.lower().endswith(".json"):
        mimetype = "application/json"

    else:
        raise ValueError(f"Unsupported deployspec file type: {package_key}")

    # Download the artefactss from the Package store

    region = task_payload.package.bucket_region
    bucket_name = task_payload.package.bucket_name

    # Get the storage location and download the single file
    bucket = MagicS3Client.get_bucket(Region=region, BucketName=bucket_name)

    # If there is a package.zip file in this folder, we can process it.
    fileobj = io.BytesIO()
    bucket.download_fileobj(Key=package_key, Fileobj=fileobj)

    # Reset Buffer Position
    fileobj.seek(0)  # Reset the pointer to the beginning so we can begin processing the zip

    specs: dict[str, DeploySpec] = {}

    # if the process_func failes with an error, we should log it and return an empty specs dict

    name = os.path.basename(package_key)
    if name in spec_mapping:
        log.info("Loading spec name={}", name)
        process_func, task = spec_mapping[name]
        data = process_func(fileobj)
        specs[task] = DeploySpec(actions=data)
    else:
        data = fileobj.read()

    # Upload the files to the artifacts store

    region = task_payload.actions.bucket_region
    bucket_name = task_payload.actions.bucket_name

    # Get the storage location and download the single file
    bucket = MagicS3Client.get_bucket(Region=region, BucketName=bucket_name)

    key = task_payload.deployment_details.get_artefacts_key(name)

    log.info("Uploading file: {})", key)

    if mimetype == "application/yaml":
        data = util.to_yaml(data)
    elif mimetype == "application/json":
        data = util.to_json(data)

    bucket.put_object(Key=key, Body=data, ServerSideEncryption="AES256")

    # Process deployspec
    if not specs:
        raise Exception("Package does not contain any deployspec files, cannot continue")

    return specs


def __load_deployspec_zip(task_payload: TaskPayload) -> dict[str, DeploySpec]:
    """
    Process package for zip file.

    Package details will contain the location of the package.zip file.
    This routine will extract the package.zip file and process the contents.
    If it finds a deployspec.yaml file, it will process that.

    It will return the actions generated as an index of tasks.

    :param task_payload: The task payload containing package and deployment details
    :type task_payload: TaskPayload
    :returns: Dictionary of deployment specifications keyed by task type
    :rtype: dict[str, DeploySpec]
    :raises ValueError: If package key is required but not provided

    Examples
    --------
    >>> task_payload = TaskPayload(
    ...     package=PackageDetails(
    ...         bucket_name="my-bucket",
    ...         bucket_region="us-east-1",
    ...         key="deployments/package.zip"
    ...     )
    ... )
    >>> specs = load_deployspec(task_payload)
    >>> # Returns: {"deploy": DeploySpec(...), "plan": DeploySpec(...)}
    """
    package_details = task_payload.package

    region = package_details.bucket_region
    bucket_name = package_details.bucket_name
    package_key = package_details.key

    if package_key is None:
        raise ValueError("Package key is required")

    # Download file from S3
    log.info("Downloading package from storage ({}) ({})".format(bucket_name, package_key))

    # Get the storage location
    bucket = MagicS3Client.get_bucket(Region=region, BucketName=bucket_name)

    # If there is a package.zip file in this folder, we can process it. Use get_object to open a stream.
    with tempfile.NamedTemporaryFile() as temp_file:
        try:
            bucket.download_fileobj(Key=package_key, Fileobj=temp_file)

            temp_file.seek(0)

            spec = __process_package_zip(task_payload, temp_file)

        except Exception as e:
            log.error("Error processing package {}: {}", package_key, str(e))
            raise ValueError(f"Error processing package {package_key}: {str(e)}")

    return spec


def __process_package_zip(task_payload: TaskPayload, temp_file: tempfile.NamedTemporaryFile) -> dict[str, DeploySpec]:
    """
    Process the zip package copying content to the artifacts store while extracting the actions
    into a DeploySpec object. (plan, apply, deploy, or teardown)

    :param task_payload: The task payload containing deployment details
    :type task_payload: TaskPayload
    :param temp_file: Temporary file containing the zip data (seekable file object)
    :type temp_file: tempfile.NamedTemporaryFile
    :returns: Dictionary of deployment specifications keyed by task type
    :rtype: dict[str, DeploySpec]
    :raises Exception: If the package does not contain a deployspec file or is malformed

    Examples
    --------
    >>> with tempfile.NamedTemporaryFile() as temp_file:
    ...     # temp_file contains zip data downloaded from S3
    ...     specs = process_package_zip(task_payload, temp_file)
    >>> # Returns: {"deploy": DeploySpec(...), "teardown": DeploySpec(...)}
    """

    dd = task_payload.deployment_details

    # Get the artifacts location
    upload_prefix = dd.get_artefacts_key()

    # Get the bucket details for artifacts which are in the Actions or State objects
    bucket_name = task_payload.actions.bucket_name
    bucket_region = task_payload.actions.bucket_region

    # This will be returned and added to the task_payload packages
    specs: dict[str, DeploySpec] = {}

    log.debug("Extracting {} and Uploading artifact to: {}", V_PACKAGE_ZIP, upload_prefix)

    bucket = MagicS3Client.get_bucket(Region=bucket_region, BucketName=bucket_name)

    with zip.ZipFile(temp_file, "r") as zipfile_obj:

        for name in zipfile_obj.namelist():

            # as we iterate through the files, look for the spec we are interested in
            # compiling.  This will be the deployspec, planspec, applyspec, or teardownspec
            # file.  We will also upload all files to the artifacts store for documentation purposes.
            # and return the spec for further processing.
            if name in spec_mapping:

                log.info("Loading spec name={}", name)
                process_func, task = spec_mapping[name]

                with zipfile_obj.open(name) as file_in_zip:
                    # Read the file from the zip stream and convert it to a dictionary
                    # the procewss_func will be one of util.read_yaml or util.read_json
                    data = process_func(file_in_zip)

                # Group the actions by the "task" type (the task type is derrived from the file name)
                specs[task] = DeploySpec(actions=data)

                # Upload the processed data
                key = dd.get_artefacts_key(name)
                log.info("Uploading processed file: {}", key)

                # We reprocess the data dictionary into a string format for upload to the artefacts store
                if name.endswith((".yaml", ".yml")):
                    upload_data = util.to_yaml(data)
                elif name.endswith(".json"):
                    upload_data = util.to_json(data)
                else:
                    upload_data = util.to_yaml(data)  # Default to YAML (*.actions files)

                bucket.put_object(Key=key, Body=upload_data, ServerSideEncryption="AES256")

            else:
                data = zipfile_obj.read(name)

                # Upload all files to the artifacts store for documentation purposes
                # and this includes the CloudFormation templates needed for the actions.

                key = dd.get_artefacts_key(name)
                log.info("Uploading file: {})", key)

                bucket.put_object(Key=key, Body=data, ServerSideEncryption="AES256")

    # Process deployspec
    if not specs:
        raise Exception("Package does not contain any deployspec files, cannot continue")

    return specs


def get_accounts_regions(action_resource: ActionResource) -> tuple[list[str], list[str]]:
    """
    Compile a list of accounts and regions for the action.

    We will combine the fields `account` and `accounts` into a single list.
    We will combine the fields `region` and `regions` into a single list.

    If no region is specified, then the default region will be used.

    :param action_resource: The action specification to extract accounts and regions from
    :type action_resource: ActionResource
    :returns: Tuple containing lists of accounts and regions
    :rtype: tuple[list[str], list[str]]

    Examples
    --------
    >>> action_resource = ActionResource(
    ...     spec={"account": "123456789012", "region": "us-east-1"}
    ... )
    >>> accounts, regions = get_accounts_regions(action_resource)
    >>> # Returns: (["123456789012"], ["us-east-1"])
    """
    accounts = action_resource.spec.get("accounts") or action_resource.spec.get("Accounts") or []
    account = action_resource.spec.get("account") or action_resource.spec.get("Account")
    if account and account not in accounts:
        accounts.append(account)

    regions = action_resource.spec.get("regions") or action_resource.spec.get("Regions") or []
    region = action_resource.spec.get("region") or action_resource.spec.get("Region") or util.get_region()
    if region and region not in regions:
        regions.append(region)

    return accounts, regions


def get_region_account_labels(action_resource: ActionResource) -> list[str]:
    """
    Generate a unique list of labels for the action specification
    for each account/region permutation.

    :param action_resource: The action specification to generate labels for
    :type action_resource: ActionResource
    :returns: List of generated labels for account/region combinations
    :rtype: list[str]

    Examples
    --------
    >>> action_resource = ActionResource(
    ...     label="create-vpc",
    ...     spec={"accounts": ["123", "456"], "regions": ["us-east-1", "us-west-2"]}
    ... )
    >>> labels = get_region_account_labels(action_resource)
    >>> # Returns: ["create-vpc-123-us-east-1", "create-vpc-123-us-west-2",
    >>> #           "create-vpc-456-us-east-1", "create-vpc-456-us-west-2"]
    """
    accounts, regions = get_accounts_regions(action_resource)

    labels = [__get_action_name(action_resource, account, region) for account in accounts for region in regions]

    return labels


def __get_action_name(action_resource: ActionResource, account: str, region: str) -> str:
    """
    Generate a unique action name based on the action specification, account, and region.

    :param action_resource: The action specification to generate the name for
    :type action_resource: ActionResource
    :param account: The AWS account ID for this action
    :type account: str
    :param region: The AWS region for this action
    :type region: str
    :returns: The generated action name
    :rtype: str

    Examples
    --------
    >>> action_resource = ActionResource(label="create-vpc")
    >>> name = __get_action_name(action_resource, "123456789012", "us-east-1")
    >>> # Returns: "create-vpc-123456789012-us-east-1"
    """
    return f"{action_resource.label}-{account}-{region}"


def compile_deployspec(task_payload: TaskPayload, deployspec: DeploySpec) -> list[ActionResource]:
    """
    Convert deployspec into an actions list.

    :param task_payload: The task payload containing deployment context
    :type task_payload: TaskPayload
    :param deployspec: The deployspec to compile into actions.  If None, it will use the deployspec from the task payload package.
    :type deployspec: DeploySpec | None
    :returns: List of compiled action specifications
    :rtype: list[ActionResource]
    :raises ValueError: If unknown action type is encountered

    Examples
    --------
    >>> deployspec = DeploySpec(actions=[
    ...     ActionResource(type="create_stack", label="vpc", spec={...})
    ... ])
    >>> actions = compile_deployspec(task_payload, deployspec)
    >>> # Returns: [ActionResource(...)]

    >>> task_payload = TaskPayload(..., package=PackageDetails(..., deployspec=deployspec))
    >>> actions = compile_deployspec(task_payload)
    >>> # Returns: [ActionResource(...)]
    """
    if deployspec is None:
        raise ValueError("Deployspec is required to compile actions")

    spec_label_map = get_spec_label_map(deployspec.actions)

    log.debug("spec_label_map", details=spec_label_map)

    # For the actions specified in the deployspec, compile them into a list of actions for the core_execute module
    compiled_actions: list[ActionResource] = []
    for action_resource in deployspec.actions:
        if not ActionFactory.is_valid_action(action_resource.kind):
            raise ValueError(f"Unknown action type {action_resource.kind}")
        compiled_actions.extend(compile_action(action_resource, task_payload, spec_label_map))
    return compiled_actions


def get_spec_label_map(actions: list[ActionResource]) -> dict[str, list[str]]:

    spec_label_map: SpecLabelMapType = {}
    for action_resource in actions:
        spec_label_map[action_resource.label] = get_region_account_labels(action_resource)
    return spec_label_map


def compile_action(
    action_resource: ActionResource, task_payload: TaskPayload, spec_label_map: SpecLabelMapType
) -> list[ActionResource]:
    """
    Compile a single action specification into executable actions.

    :param action_resource: The action specification to compile
    :type action_resource: ActionResource
    :param task_payload: The task payload containing deployment context
    :type task_payload: TaskPayload
    :param spec_label_map: Mapping of spec labels to region/account combinations
    :type spec_label_map: SpecLabelMapType
    :returns: List of compiled action specifications
    :rtype: list[ActionResource]
    :raises ValueError: If required account/region information is missing or invalid

    Examples
    --------
    >>> action_resource = ActionResource(type="create_stack", label="vpc", spec={...})
    >>> actions = compile_action(action_resource, task_payload, spec_label_map,
    ...                         allow_multiple_stacks=True, kind=CreateStackActionResource)
    >>> # Returns: [ActionResource(...)]
    """
    accounts, regions = get_accounts_regions(action_resource)

    action_list: list[ActionResource] = []
    for account in accounts:
        for region in regions:
            execute_action = generate_action_command(task_payload, action_resource, spec_label_map, account, region)
            action_list.append(execute_action)
    return action_list


def generate_action_command(
    task_payload: TaskPayload,
    action_resource: ActionResource,
    spec_label_map: SpecLabelMapType,
    account: str,
    region: str,
) -> ActionResource:
    """
    Generate an executable action command from an action specification.

    :param task_payload: The task payload containing deployment context
    :type task_payload: TaskPayload
    :param action_resource: The action specification to generate command for
    :type action_resource: ActionResource
    :param spec_label_map: Mapping of spec labels to region/account combinations
    :type spec_label_map: SpecLabelMapType
    :param account: The AWS account ID for this action
    :type account: str
    :param region: The AWS region for this action
    :type region: str
    :returns: The generated action specification
    :rtype: ActionResource
    :raises ValueError: If action type is required but not provided

    Examples
    --------
    >>> action_resource = ActionResource(action="AWS::CreateStack", label="vpc", spec={...})
    >>> command = generate_action_command(task_payload, action_resource, spec_label_map,
    ...                                  "123456789012", "us-east-1")
    >>> # Returns: ActionResource with executable parameters
    """

    if action_resource.kind is None:
        raise ValueError("Action type is required that matches a valid Action model")

    klass = ActionFactory.get_action_class(action_resource.kind)
    if klass is None:
        raise ValueError(f"Cannot find action class for {action_resource.kind}")

    spec = deepcopy(action_resource.spec)

    __delkeys(["account", "region", "accounts", "regions", "Accounts", "Regions"], spec)

    spec["account"] = account
    spec["region"] = region

    # Validate Parameters
    spec = klass.generate_action_parameters(**spec)

    # Check if the pydantic model has the "TemplateUrl" field, if it does
    # update the path to the bucket deployment details.
    if hasattr(spec, "template_url"):
        spec.template_url = __get_action_template_url(
            action_resource,
            task_payload.actions.bucket_name,
            task_payload.actions.bucket_region,
            task_payload.deployment_details,
        )

    if hasattr(spec, "parameters"):
        __apply_syntax_update(spec.parameters)

    if hasattr(spec, "tags"):
        # Add default tags to all actions
        spec.tags = __get_tags(action_resource.scope, task_payload.deployment_details, spec.tags)

    # Validate ActionResource.  Note, the "Kind" field is automatically updated in generate_action_resource
    execute_action = klass.generate_action_resource(
        **{
            "Name": __get_action_name(action_resource, account, region),
            "DependsOn": __get_depends_on(action_resource, spec_label_map),
            "Spec": spec.model_dump(),
        }
    )

    return execute_action


def __get_action_template_url(
    action_resource: ActionResource,
    bucket_name: str,
    bucket_region: str,
    deployment_details: DeploymentDetails,
) -> str | None:
    """
    Get the template URL for a CloudFormation action.

    :param action_resource: The action specification containing template parameters
    :type action_resource: ActionResource
    :param bucket_name: The S3 bucket name where templates are stored
    :type bucket_name: str
    :param bucket_region: The AWS region of the S3 bucket
    :type bucket_region: str
    :param deployment_details: The deployment details for path generation
    :type deployment_details: DeploymentDetails
    :returns: The template URL or None if no template specified
    :rtype: str | None

    Examples
    --------
    >>> action_resource = ActionResource(spec={"template_url": "vpc.yaml"})
    >>> url = __get_action_template_url(action_resource, "my-bucket", "us-east-1", deployment_details)
    >>> # Returns: "s3://my-bucket/artifacts/portfolio/app/branch/build/vpc.yaml"
    """

    key = __getany(action_resource.spec, ["template_url", "TemplateUrl", "template", "Template"])
    if key is None:
        return None
    scope = __get_action_scope(action_resource, deployment_details)

    return __get_template_url(bucket_name, bucket_region, deployment_details, key, scope)


def __get_template_url(
    bucket_name: str,
    bucket_region: str,
    dd: DeploymentDetails,
    template: str | None = None,
    scope: str | None = None,
) -> str:
    """
    Generate the full template URL for CloudFormation templates.

    :param bucket_name: The S3 bucket name where templates are stored
    :type bucket_name: str
    :param bucket_region: The AWS region of the S3 bucket
    :type bucket_region: str
    :param dd: The deployment details for path generation
    :type dd: DeploymentDetails
    :param template: The template file name (optional)
    :type template: str | None
    :param scope: The deployment scope for path generation (optional)
    :type scope: str | None
    :returns: The complete template URL
    :rtype: str

    Examples
    --------
    >>> url = __get_template_url("my-bucket", "us-east-1", deployment_details,
    ...                        "vpc.yaml", "build")
    >>> # Returns: "s3://my-bucket/artifacts/portfolio/app/branch/build/vpc.yaml"
    """

    if not template:
        template = ""

    template = os.path.basename(template)

    store = util.get_storage_volume(bucket_region)

    sep = "/" if util.is_use_s3() else os.path.sep

    return f"{store}{sep}{bucket_name}{sep}{dd.get_artefacts_key(template, scope)}"


def get_context(task_payload: TaskPayload) -> dict:
    """
    Get the context for the Jinja2 templating.

    :param task_payload: The task payload object containing deployment details
    :type task_payload: TaskPayload
    :returns: The context dictionary for Jinja2 templating
    :rtype: dict
    :raises Exception: If error occurs getting facts for deployment context

    Examples
    --------
    >>> context = get_context(task_payload)
    >>> # Returns: {"core": {"portfolio": {...}, "app": {...}, ...}}
    """
    deployment_details = task_payload.deployment_details

    try:
        # Get the facts for the deployment
        state = get_facts(deployment_details)
    except Exception as e:
        log.error("Error getting facts for deployspec context: {}", e)
        raise

    return {CONTEXT_ROOT: state}


def apply_context(actions: list[ActionResource], context: dict) -> list[ActionResource]:
    """
    Apply state to the actions list. Uses Jinja Template to render the state.

    :param actions: The list of action specifications to render
    :type actions: list[ActionResource]
    :param context: The context dictionary for template rendering
    :type context: dict
    :returns: The rendered actions list with context applied
    :rtype: list[ActionResource]
    :raises ValueError: If unknown action type is encountered in actions list

    Examples
    --------
    >>> actions = [ActionResource(spec={"StackName": "{{ core.portfolio }}-vpc"})]
    >>> context = {"core": {"portfolio": "web-services"}}
    >>> rendered = apply_context(actions, context)
    >>> # Returns: [ActionResource(spec={"StackName": "web-services-vpc"})]
    """

    actions_list: list[dict[str, Any]] = [a.model_dump() for a in actions]

    try:
        unrendered_contents = util.to_yaml(actions_list)

        renderer = Jinja2Renderer()

        # Render the template with the provided state.  I don't know
        # if the root should be "core" or if the root should be "context".
        # If we change the root to "context", then we need to change the
        # input to template.render(context=context[CONTEXT_ROOT])
        rendered_contents = renderer.render_string(unrendered_contents, context[CONTEXT_ROOT])

        action_list = util.from_yaml(rendered_contents)

        # Convert the action list back to ActionResource objects
        # Please note that the value of action.kind and action.spec is NOT validated here.
        actions: list[ActionResource] = []
        for action in action_list:
            if isinstance(action, dict):
                actions.append(ActionResource(**action))
            elif isinstance(action, ActionResource):
                actions.append(action)
            else:
                raise ValueError(f"Unknown action type {type(action)} in actions list")

        log.debug("Compiled actions: {}", actions)

    except Exception as e:
        # Enhanced error logging for Jinja2 errors
        error_details = {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "context_keys": list(context.keys()) if context else [],
        }

        # Add Jinja2-specific error details
        import jinja2

        if isinstance(e, jinja2.TemplateError):
            error_details.update(
                {
                    "jinja2_error_type": type(e).__name__,
                    "template_name": getattr(e, "name", "unknown"),
                    "line_number": getattr(e, "lineno", "unknown"),
                    "template_error": True,
                }
            )

            # Log template syntax errors with more detail
            if isinstance(e, jinja2.TemplateSyntaxError):
                error_details.update(
                    {
                        "syntax_error": True,
                        "error_location": (f"line {e.lineno}" if e.lineno else "unknown location"),
                    }
                )
                log.error(
                    "Jinja2 Template Syntax Error at {}: {}",
                    error_details["error_location"],
                    e.message,
                )

            # Log undefined variable errors
            elif isinstance(e, jinja2.UndefinedError):
                error_details.update({"undefined_error": True, "undefined_variable": str(e)})
                log.error("Jinja2 Undefined Variable Error: {}", str(e))

            # Log template runtime errors
            elif isinstance(e, jinja2.TemplateRuntimeError):
                error_details.update({"runtime_error": True})
                log.error("Jinja2 Template Runtime Error: {}", str(e))

            # Log template assertion errors
            elif isinstance(e, jinja2.TemplateAssertionError):
                error_details.update({"assertion_error": True})
                log.error("Jinja2 Template Assertion Error: {}", str(e))

            # Log security errors
            elif isinstance(e, jinja2.SecurityError):
                error_details.update({"security_error": True})
                log.error("Jinja2 Security Error: {}", str(e))

            else:
                log.error("Jinja2 Template Error ({}): {}", type(e).__name__, str(e))

        # Log the template content for debugging (truncated if too long)
        try:
            template_preview = unrendered_contents[:500] + "..." if len(unrendered_contents) > 500 else unrendered_contents
            error_details["template_preview"] = template_preview
            log.debug("Template content preview: {}", template_preview)
        except:
            log.debug("Could not log template content preview")

        # Log available context for debugging
        if context and CONTEXT_ROOT in context:
            context_preview = (
                str(context[CONTEXT_ROOT])[:300] + "..." if len(str(context[CONTEXT_ROOT])) > 300 else str(context[CONTEXT_ROOT])
            )
            error_details["context_preview"] = context_preview
            log.debug("Context preview: {}", context_preview)

        # Log comprehensive error details
        log.error("Error applying context to actions", details=error_details)

        # Re-raise with enhanced error message
        raise ValueError(f"Error applying context to actions: {str(e)}")

    # Return the rendered actions list
    return actions


def __get_tags(
    scope: str | None,
    deployment_details: DeploymentDetails,
    user_tags: dict[str, str] | None,
) -> dict | None:
    """
    Generate AWS tags based on deployment scope and details.

    :param scope: The deployment scope (portfolio, app, branch, build)
    :type scope: str | None
    :param deployment_details: The deployment details containing tag information
    :type deployment_details: DeploymentDetails
    :param user_tags: User-provided tags that override deployment_details tags
    :type user_tags: dict[str, str] | None
    :returns: Dictionary of AWS tags or None if no tags
    :rtype: dict | None

    Examples
    --------
    >>> tags = __get_tags("build", deployment_details, {"Environment": "prod"})
    >>> # Returns: {"Portfolio": "core", "App": "api", "Branch": "master", "Build": "1234", "Environment": "prod"}
    """
    tags: dict[str, str] = deployment_details.tags or {}

    if not scope:
        scope = SCOPE_BUILD

    if deployment_details.portfolio:
        tags[TAG_PORTFOLIO] = deployment_details.portfolio

    if scope in [SCOPE_APP, SCOPE_BRANCH, SCOPE_BUILD] and deployment_details.app:
        tags[TAG_APP] = deployment_details.app

    if scope in [SCOPE_BRANCH, SCOPE_BUILD] and deployment_details.branch:
        tags[TAG_BRANCH] = deployment_details.branch

    if scope == SCOPE_BUILD and deployment_details.build:
        tags[TAG_BUILD] = deployment_details.build

    if isinstance(user_tags, dict):
        tags.update(user_tags)

    return tags if len(tags) > 0 else None


def __apply_syntax_update(parameters: dict | None) -> dict | None:
    """
    Deal with runner syntax changes for backward compatibility.

    Converts old syntax: ``S3ComplianceBucketName: "{{ foo.bar }}"``
    To new syntax: ``S3ComplianceBucketName: "{{ 'foo/bar' | lookup }}"``

    :param parameters: The stack parameters dictionary to update
    :type parameters: dict | None
    :returns: The updated stack parameters with new syntax
    :rtype: dict | None

    Examples
    --------
    >>> spec = {"BucketName": "{{ portfolio.name }}"}
    >>> updated = __apply_syntax_update(spec)
    >>> # Returns: {"BucketName": "{{ 'portfolio/name' | lookup }}"}
    """
    if not parameters:
        return None

    # NOTE:  See this is expecting "core" to be the root.
    # See the "apply_context" function.  It's not clear if the root should be "core" or "context".
    for key in parameters:
        parameters[key] = re.sub(
            r"{{ (?!core.)([^.]*)\.(.*) }}",
            r'{{ "\1/\2" | lookup }}',
            str(parameters[key]),
        )
    return parameters


def __get_depends_on(action: ActionResource, spec_label_map: SpecLabelMapType) -> list:
    """
    Get the dependency list for an action specification.

    :param action: The action specification to get dependencies for
    :type action: ActionResource
    :param spec_label_map: Mapping of spec labels to region/account combinations
    :type spec_label_map: SpecLabelMapType
    :returns: List of action labels this action depends on
    :rtype: list

    Examples
    --------
    >>> action = ActionResource(depends_on=["vpc", "security"])
    >>> deps = __get_depends_on(action, spec_label_map)
    >>> # Returns: ["vpc-123-us-east-1", "security-123-us-east-1"]
    """

    if not action.depends_on:
        return []

    depends_on: list = [item for sublist in map(lambda name: spec_label_map[name], action.depends_on) for item in sublist]

    return depends_on


def __get_action_scope(action: ActionResource, deployment_details: DeploymentDetails) -> str:
    """
    Determine the deployment scope for an action based on stack name templates.

    Relies on the deployspec to have templating to determine the scope or you can specify
    the scope in the action object.

    Example stack_name: ``"{{ core.Project }}-{{ core.App }}-resources"``
    The above will return the scope of SCOPE_APP ('app').

    :param action: The action specification to determine scope for
    :type action: ActionResource
    :param deployment_details: The deployment details containing scope information
    :type deployment_details: DeploymentDetails
    :returns: The deployment scope (portfolio, app, branch, build)
    :rtype: str

    Examples
    --------
    >>> action = ActionResource(spec={"stack_name": "{{ core.Portfolio }}-{{ core.App }}-vpc"})
    >>> scope = __get_action_scope(action, deployment_details)
    >>> # Returns: "app"
    """

    if action.scope:
        return action.scope

    if deployment_details.scope:
        return deployment_details.scope

    stack_name = __getany(action.spec, ["stack_name", "StackName"], "")

    return __get_stack_scope(stack_name)


def __get_stack_scope(stack_name: str) -> str:
    """
    Return the scope based on stack name Jinja2 placeholder variables.

    Example: ``stack_name = "{{ core.Project }}-{{ core.App }}-resources"``

    :param stack_name: The Jinja2 stack name template to analyze
    :type stack_name: str
    :returns: The deployment scope (defaults to SCOPE_BUILD if not determinable)
    :rtype: str

    Examples
    --------
    >>> scope = __get_stack_scope("{{ core.Portfolio }}-{{ core.App }}-vpc")
    >>> # Returns: "app"

    >>> scope = __get_stack_scope("{{ core.Build }}-resources")
    >>> # Returns: "build"
    """
    # Determine stack scope for tagging
    if DD_BUILD in stack_name:
        scope = SCOPE_BUILD
    elif DD_BRANCH in stack_name:
        scope = SCOPE_BRANCH
    elif DD_APP in stack_name:
        scope = SCOPE_APP
    elif DD_PORTFOLIO in stack_name or "Project" in stack_name:
        scope = SCOPE_PORTFOLIO
    else:
        scope = SCOPE_BUILD

    log.debug("Build scope={}, stack_name={}", scope, stack_name)

    return scope


def __getany(data: dict, keys: list[str], default: Any = None) -> Any:
    """Returns value for the first key it finds with a non-Empty value or the specified default"""
    for key in keys:
        value = data.get(key)
        if value:
            return value
    return default


def __delkeys(keys: list, data: dict) -> None:
    """Mutates data by deleting keys you specify"""
    for key in keys:
        if key in data:
            del data[key]

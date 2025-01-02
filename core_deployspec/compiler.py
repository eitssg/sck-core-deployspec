"""Description: Compile a deployspec package into actions and templates.

- Extracts package files to a location in S3
- Parses and compiles the package deployspec (deployspec.yml)
- Uploads actions to S3
"""

import io
import os
import re
import zipfile as zip
from ruamel import yaml
import json
import core_logging as log

import core_framework as util

from jinja2 import Template

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
    V_EMPTY,
    V_PACKAGE_ZIP,
)

from core_db.facter import get_facts

from core_framework.models import (
    ActionDefinition,
    ActionParams,
    TaskPayload,
    DeploySpec,
    ActionSpec,
    DeploymentDetails,
)

from core_helper.magic import MagicS3Client


SpecLabelMapType = dict[str, list[str]]

CONTEXT_ROOT = "core"


def load_deployspec(task_payload: TaskPayload) -> DeploySpec:
    """
    Process package For local mode.

    Package details will contain the location of the package.zip file.

    This routine will extract the package.zip file and process the contents.  If it finds a deployspec.yaml
    file, it will process that.

    It will mutate package_details to include the DeploySpec object.

    Args:
        package_details (PackageDetails): The package details haveing the location of the deployspec.
        upload_prefix (str): The upload prefix path.  This is in the artefact key prefix
        deployment_type (str): The deployment type: deploysepc, planspec, applyspec, teardownspec

    Returns:
        DeploySpec: The deployspec object
    """
    package_details = task_payload.Package

    region = package_details.BucketRegion
    bucket_name = package_details.BucketName
    package_key = package_details.Key

    if package_key is None:
        raise ValueError("Package key is required")

    # Download file from S3
    log.info(
        "Downloading package from storage ({}) ({})".format(bucket_name, package_key)
    )

    # Get the storage location
    bucket = MagicS3Client.get_bucket(Region=region, BucketName=bucket_name)

    # If there is a packge.zip file in this folder, we can process it.
    zip_fileobj = io.BytesIO()
    bucket.download_fileobj(Key=package_key, Fileobj=zip_fileobj)

    # We have read the entire zip file into memory.  I hope it's not too big!!
    # Process the zip file and extract the deployspec file.
    spec = process_package_zip(task_payload, zip_fileobj, os.path.sep)

    # Mutate package_details to include the DeploySpec

    return spec


def get_deployment_type(task: str) -> str:
    if task == "deploy":
        return "deployspec"
    elif task == "plan":
        return "planspec"
    elif task == "apply":
        return "applyspec"
    elif task == "teardown":
        return "teardownspec"
    else:
        raise ValueError(f"Invalid task: {task}")


def process_package_zip(
    task_payload: TaskPayload,
    zip_fileobj: io.BytesIO,
    sep: str = "/",
) -> DeploySpec:
    """
    Process the zip package copying content to the artefacts store while extraction the actions
    into a DeploySpec object. (plan, appl, deploy, or teardown)

    Args:
        zip_fileobj (io.BytesIO): io stream of the zip file
        bucket (Any): a MagicBucket or boto3 bucket object
        upload_prefix (str): where in the store to upload
        deployment_type (str): deployspec, planspec, applyspec, teardownspec
        sep (str, optional): zip files will have "/", do we replace?. Defaults to "/".

    Raises:
        Exception: if the package does not contain a deployspec file or is malformed.

    Returns:
        DeploySpec: the deployspec object with the deployspec, plansepec, applyspec, or teardownspec actions
    """

    deployment_details = task_payload.DeploymentDetails

    # Get the artefacts location
    upload_prefix = deployment_details.get_artefacts_key()

    # Get the bucket details for artefacts which are in the Actions or State objects
    bucket_name = task_payload.Actions.BucketName
    bucket_region = task_payload.Actions.BucketRegion

    deployment_type = get_deployment_type(task_payload.Task)

    # This will be returned and added to the task_payload packages
    spec: DeploySpec | None = None

    zipfile = zip.ZipFile(zip_fileobj, "r")

    log.debug(
        "Extracting {} and Uploading artefact to: {}", V_PACKAGE_ZIP, upload_prefix
    )

    bucket = MagicS3Client.get_bucket(Region=bucket_region, BucketName=bucket_name)

    for name in zipfile.namelist():

        # as we iterate through the files, look for the spec we are interested in
        # compiling.  This will be the deployspec, planspec, applyspec, or teardownspec
        # file.  We will also upload all files to the artefacts store for documentation purposes.
        # and return the spec for further processing.
        if name == f"{deployment_type}.yaml":

            log.info("Loading deployspec name={}", name)

            y = yaml.YAML(typ="rt")
            data = y.load(zipfile.read(name))
            spec = DeploySpec(actions=data)

        elif name == f"{deployment_type}.json":

            log.info("Loading deployspec name={}", name)

            data = json.loads(zipfile.read(name))
            spec = DeploySpec(actions=data)

        # Upload all files to the artefacts store for documentation purposes
        # and this includes the cloduformation templates needed for the actions.

        key = f"{upload_prefix}{sep}{name}"
        data = zipfile.read(name)

        log.info("Uploading file: {})", key)

        bucket.put_object(Key=key, Body=data, ServerSideEncryption="AES256")

    # Process deployspec
    if not spec:
        raise Exception(
            f"Package does not contain a {deployment_type} file, cannot continue"
        )

    task_payload.Package.DeploySpec = spec

    return spec


def get_accounts_regions(action_spec: ActionSpec) -> tuple[list[str], list[str]]:
    """
    Compile a list of accounts and regions for the action.

    We will combine the fields `account` and `accounts` into a single list.
    We will combine the fields `region` and `regions` into a single list.

    if no region is specified, then the default region will be used.

    Args:
        action (ActionSpec): The action to compile.

    Returns:
        tuple[list[str], list[str]]: a list of accounts and a list of regions.
    """
    accounts = action_spec.params.accounts or []
    if action_spec.params.account and action_spec.params.account not in accounts:
        accounts.append(action_spec.params.account)

    regions = action_spec.params.regions or []
    if action_spec.params.region:
        if action_spec.params.region not in regions:
            regions.append(action_spec.params.region)
    else:
        default_region = util.get_region()
        if default_region not in regions:
            regions.append(default_region)

    return accounts, regions


def get_region_account_labels(action_spec: ActionSpec) -> list[str]:
    """
    Generate a unique list of labels for the action specification
    for each account/region permuation.

    Args:
        action_spec (ActionSpec): The action specification

    Returns:
        list[str]: List of labels
    """
    accounts, regions = get_accounts_regions(action_spec)

    labels = [
        f"{action_spec.label}-{account}-{region}"
        for account in accounts
        for region in regions
    ]

    return labels


def compile_deployspec(
    task_payload: TaskPayload, deployspec: DeploySpec
) -> list[ActionDefinition]:
    """
    Convert deployspec into an actions list.

    Args:
        task_payload (dict): The task payload.
        deployspec (DeploySpec): The deployspec to compile.

    Returns:
        list: The compiled actions.
    """
    spec_label_map: SpecLabelMapType = {}
    for action_spec in deployspec.action_specs:
        spec_label_map[action_spec.label] = get_region_account_labels(action_spec)

    log.debug("spec_label_map", details=spec_label_map)

    # Catalog of know Deployspec types
    routes: dict[str, dict] = {
        "create_stack": {"allow_multiple_stacks": True},
        "delete_stack": {"allow_multiple_stacks": True},
        "create_user": {"allow_multiple_stacks": False},
        "delete_user": {"allow_multiple_stacks": False},
        "create_change_set": {"allow_multiple_stacks": False},
        "apply_change_set": {"allow_multiple_stacks": False},
        "delete_change_set": {"allow_multiple_stacks": False},
    }

    # For the actions specified in the deployspec, compile them into a list of actions for the core_execute module
    compiled_actions: list[ActionDefinition] = []
    for action_spec in deployspec.action_specs:
        params = routes.get(action_spec.type, None)
        if not params:
            raise ValueError(f"Unknown action type {action_spec.type}")
        compiled_actions.extend(
            compile_action(action_spec, task_payload, spec_label_map, **params)
        )
    return compiled_actions


def compile_action(
    action_spec: ActionSpec,
    task_payload: TaskPayload,
    spec_label_map: SpecLabelMapType,
    allow_multiple_stacks: bool = False,
) -> list[ActionDefinition]:

    accounts, regions = get_accounts_regions(action_spec)

    if not allow_multiple_stacks:
        if len(accounts) == 0 or len(regions) == 0:
            raise ValueError("Missing account or region")

        if len(accounts) > 1 or len(regions) > 1:
            raise ValueError(
                f"Cannot {action_spec.type} from multiple accounts or regions"
            )

    action_list: list[ActionDefinition] = []
    for account in accounts:
        for region in regions:
            action_list.append(
                generate_action_command(
                    task_payload, action_spec, spec_label_map, account, region
                )
            )
    return action_list


def generate_action_command(
    task_payload: TaskPayload,
    action_spec: ActionSpec,
    spec_label_map: SpecLabelMapType,
    account: str,
    region: str,
) -> ActionDefinition:

    if action_spec.action is None:
        raise ValueError("Action type is required that matches a valid Action model")

    deployment_details = task_payload.DeploymentDetails
    package_details = task_payload.Package
    bucket_name = package_details.BucketName
    bucket_region = package_details.BucketRegion
    label = f"{action_spec.label}-{account}-{region}"
    depends_on = __get_depends_on(action_spec, spec_label_map)
    scope = __get_action_scope(action_spec, deployment_details)

    # These are the minimum requred fields.
    execute_action = ActionDefinition(
        Label=label,
        Type=action_spec.action,
        DependsOn=depends_on,
        Params=ActionParams(Account=account, Region=region),
        Scope=scope,
    )

    # These fields are optional depending on the actoin being generated. At the moment, we do not have vlidators.
    # If the action will fail, you'll find out in the core_execute module.
    user_name = action_spec.params.user_name
    if user_name:
        execute_action.Params.UserName = user_name

    stack_name = action_spec.params.stack_name
    if stack_name:
        execute_action.Params.StackName = stack_name

    template_url = get_action_template_url(
        action_spec, bucket_name, bucket_region, deployment_details
    )
    if template_url:
        execute_action.Params.TemplateUrl = template_url

    stack_parameters = __apply_syntax_update(action_spec.params.parameters)
    if stack_parameters:
        execute_action.Params.StackParameters = stack_parameters

    tags = __get_tags(scope, deployment_details)
    if tags:
        execute_action.Params.Tags = tags

    policy = __get_stack_policy_json(action_spec.params.stack_policy)
    if policy:
        execute_action.Params.StackPolicy = json.loads(policy)

    return execute_action


def get_action_template_url(
    action_spec: ActionSpec,
    bucket_name: str,
    bucket_region: str,
    deployment_details: DeploymentDetails,
) -> str | None:

    if action_spec.params.template is None:
        return None
    key = action_spec.params.template
    scope = __get_action_scope(action_spec, deployment_details)

    return get_template_url(bucket_name, bucket_region, deployment_details, key, scope)


def get_template_url(
    bucket_name: str,
    bucket_region: str,
    dd: DeploymentDetails,
    template: str | None = None,
    scope: str | None = None,
) -> str:

    if not template:
        template = ""

    store = util.get_storage_volume(bucket_region)

    sep = "/" if util.is_use_s3() else os.path.sep

    return f"{store}{sep}{bucket_name}{sep}{dd.get_artefacts_key(template, scope)}"


def upload_actions(task_payload: TaskPayload, actions_body: str) -> tuple[str, str]:
    """
    Upload the compiled actions to the target defined in deployment details

    Args:
        task_payload (dict): The task payload contianng the deployment details and package details
        actions_output (str): The compiled actions in yaml or JSON format

    Returns:
        tuple: returns the action key and version
    """

    actions_details = task_payload.Actions
    bucket_name = actions_details.BucketName
    bucket_region = actions_details.BucketRegion

    actions_key = actions_details.Key

    log.debug(
        "Uploading actions file to actions bucket [{}]: (key: {})",
        bucket_name,
        actions_key,
    )

    bucket = MagicS3Client.get_bucket(Region=bucket_region, BucketName=bucket_name)
    object = bucket.put_object(
        Body=actions_body.encode("utf-8"),
        Key=actions_key,
        ServerSideEncryption="AES256",
        ACL="bucket-owner-full-control",
    )
    actions_version = object.version_id

    task_payload.Actions.VersionId = actions_version

    return actions_key, actions_version


def get_context(task_payload: TaskPayload) -> dict:
    """
    Get the context for the Jinja2 templating

    Args:
        task_payload (TaskPayload): The task payload object

    Returns:
        dict: The context for the Jinja2 templating
    """
    deployment_details = task_payload.DeploymentDetails

    try:
        # Get the facts for the deployment
        state = get_facts(deployment_details)
    except Exception as e:
        log.error("Error getting facts for deployspec context: {}", e)
        raise

    return {CONTEXT_ROOT: state}


def apply_context(actions_list: list[dict], context: dict) -> str:
    """
    Apply state to the ctions list.  Uses Jinja Template to render the state.

    Args:
        deployspec_contents (str): The deployspec file contents.
        state (dict): The state to apply to the deployspec.

    Returns:
        str: The deployspec file contents with the state modifiecations applied
    """

    unrendered_contents = util.to_yaml(actions_list)

    # Create a Jinja2 template from the deployspec contents
    template = Template(unrendered_contents)

    # Render the template with the provided state.  I don't know
    # if the root should be "core" or if the root should be "context".
    # If we change the root to "context", then we need to change the
    # input to template.render(context=context[CONTEXT_ROOT])
    rendered_contents = template.render(core=context[CONTEXT_ROOT])

    return rendered_contents


def __get_stack_policy_json(stack_policy: str | dict | None) -> str | None:
    """
    If a policy has been defined, we will return the policy as a JSON string.

    Args:
        stack_policy (str | dict | None): The input polic str or dict

    Returns:
        str | None: a policy statement in JSON format
    """
    if not stack_policy:
        return None
    if isinstance(stack_policy, dict) or isinstance(stack_policy, str):
        return util.to_json(stack_policy)
    else:
        return None


def __get_tags(scope: str | None, deployment_details: DeploymentDetails) -> dict | None:

    if not scope or scope == V_EMPTY:
        scope = SCOPE_BUILD

    tags: dict[str, str] = deployment_details.Tags or {}

    if deployment_details.Portfolio:
        tags[TAG_PORTFOLIO] = deployment_details.Portfolio

    if scope in [SCOPE_APP, SCOPE_BRANCH, SCOPE_BUILD] and deployment_details.App:
        tags[TAG_APP] = deployment_details.App

    if scope in [SCOPE_BRANCH, SCOPE_BUILD] and deployment_details.Branch:
        tags[TAG_BRANCH] = deployment_details.Branch

    if scope == SCOPE_BUILD and deployment_details.Build:
        tags[TAG_BUILD] = deployment_details.Build

    return tags if len(tags) > 0 else None


def __apply_syntax_update(stack_parameters: dict | None) -> dict | None:
    """

    Dealing with runner syntax changes. From:

    S3ComplianceBucketName: "{{ foo.bar }}"

    To:

    S3ComplianceBucketName: "{{ 'foo/bar' | lookup }}"

    Args:
        stack_parameters (dict): The stack parameters to update.

    Returns:
        dict: The updated stack parameters.

    """
    if not stack_parameters:
        return None

    # NOTE:  See this is expecting "core" to be the root.
    # See the "apply_state" function.  It's not clear if the root should be "core" or "context".
    for key in stack_parameters:
        stack_parameters[key] = re.sub(
            r"{{ (?!core.)([^.]*)\.(.*) }}",
            r"{{ '\1/\2' | lookup }}",
            "{}".format(
                stack_parameters[key]
            ),  # Must be a string for re.sub to work, so fails when your paramter is a number.
        )
    return stack_parameters


def __get_depends_on(action: ActionSpec, spec_label_map: SpecLabelMapType) -> list:

    if not action.depends_on:
        return []

    depends_on: list = [
        item
        for sublist in map(lambda label: spec_label_map[label], action.depends_on)
        for item in sublist
    ]

    return depends_on


def __get_action_scope(
    action: ActionSpec, deployment_details: DeploymentDetails
) -> str:
    """
    Relies on the deployspec to have templating to determine the scope or you can specify the scope in the action object

    stack_name: "{{ core.Project }}-{{ core.App }}-resources"

    The above will return the scope of SCOPE_APP ('app').

    delospec.actions[0].scope = "app"  # lowercase

    Args:
        stack_name (str): The stack name to determine the scope for.

    Returns:
        str: The scope of the stack.  This will default to SCOPE_BUILD if not deteriminable.

    """

    if action.scope:
        return action.scope

    if deployment_details.Scope:
        return deployment_details.Scope

    stack_name = action.params.stack_name or ""

    return __get_stack_scope(stack_name)


def __get_stack_scope(stack_name: str) -> str:
    """
    Return the scope bas on stack name Jinja2 placeholder variables.

    stack_name = "{{ core.Project }}-{{ core.App }}-resources" for example.

    Args:
        stack_name (str): The Jinja2 stack name template

    Returns:
        str: the SCOPE.  Defaults to SCOPE_BUILD if not determinable.

    """
    # Determine stack scope for tagging
    if DD_BUILD in stack_name:
        scope = SCOPE_BUILD
    elif DD_BRANCH in stack_name:
        scope = SCOPE_BRANCH
    elif DD_APP in stack_name:
        scope = SCOPE_APP
    elif DD_PORTFOLIO in stack_name:
        scope = SCOPE_PORTFOLIO
    elif "Project" in stack_name:
        scope = SCOPE_PORTFOLIO
    else:
        scope = SCOPE_BUILD

    log.debug("Build scope={}, stack_name={}", scope, stack_name)

    return scope

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
import json
import core_logging as log

import core_framework as util

from core_execute.actionlib.actions.aws.create_stack import CreateStackActionSpec
from core_execute.actionlib.actions.aws.delete_stack import DeleteStackActionSpec
from core_execute.actionlib.actions.aws.delete_user import DeleteUserActionSpec
from core_execute.actionlib.actions.aws.put_user import PutUserActionSpec
from core_execute.actionlib.actions.aws.delete_change_set import DeleteChangeSetActionSpec
from core_execute.actionlib.actions.aws.apply_change_set import ApplyChangeSetActionSpec
from core_execute.actionlib.actions.aws.create_change_set import CreateChangeSetActionSpec

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
    ActionSpec,
    TaskPayload,
    DeploySpec,
    ActionSpec,
    DeploymentDetails,
    ActionDetails,
    StateDetails,
)

from core_helper.magic import MagicS3Client


SpecLabelMapType = dict[str, list[str]]

CONTEXT_ROOT = "core"


def load_deployspec(task_payload: TaskPayload) -> dict[str, DeploySpec]:
    """
    Process package for local mode.

    Package details will contain the location of the package.zip file.
    This routine will extract the package.zip file and process the contents.
    If it finds a deployspec.yaml file, it will process that.

    It will mutate package_details to include the DeploySpec object.

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

    # If there is a package.zip file in this folder, we can process it.
    zip_fileobj = io.BytesIO()
    bucket.download_fileobj(Key=package_key, Fileobj=zip_fileobj)

    # We have read the entire zip file into memory.  I hope it's not too big!!
    # Process the zip file and extract the deployspec file.
    spec = process_package_zip(task_payload, zip_fileobj)

    # Mutate package_details to include the DeploySpec

    return spec


def process_package_zip(task_payload: TaskPayload, zip_fileobj: io.BytesIO) -> dict[str, DeploySpec]:
    """
    Process the zip package copying content to the artifacts store while extracting the actions
    into a DeploySpec object. (plan, apply, deploy, or teardown)

    :param task_payload: The task payload containing deployment details
    :type task_payload: TaskPayload
    :param zip_fileobj: IO stream of the zip file
    :type zip_fileobj: io.BytesIO
    :returns: Dictionary of deployment specifications keyed by task type
    :rtype: dict[str, DeploySpec]
    :raises Exception: If the package does not contain a deployspec file or is malformed

    Examples
    --------
    >>> import io
    >>> zip_data = io.BytesIO(zip_content)
    >>> specs = process_package_zip(task_payload, zip_data)
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

    zipfile_obj = zip.ZipFile(zip_fileobj, "r")

    log.debug("Extracting {} and Uploading artifact to: {}", V_PACKAGE_ZIP, upload_prefix)

    bucket = MagicS3Client.get_bucket(Region=bucket_region, BucketName=bucket_name)

    spec_mapping = {
        V_DEPLOYSPEC_FILE_YAML: (util.from_yaml, TASK_DEPLOY),
        V_DEPLOYSPEC_FILE_JSON: (util.from_json, TASK_DEPLOY),
        V_PLANSPEC_FILE_YAML: (util.from_yaml, TASK_PLAN),
        V_PLANSPEC_FILE_JSON: (util.from_json, TASK_PLAN),
        V_APPLYSPEC_FILE_YAML: (util.from_yaml, TASK_APPLY),
        V_APPLYSPEC_FILE_JSON: (util.from_json, TASK_APPLY),
        V_TEARDOWNSPEC_FILE_YAML: (util.from_yaml, TASK_TEARDOWN),
        V_TEARDOWNSPEC_FILE_JSON: (util.from_json, TASK_TEARDOWN),
    }

    for name in zipfile_obj.namelist():

        # as we iterate through the files, look for the spec we are interested in
        # compiling.  This will be the deployspec, planspec, applyspec, or teardownspec
        # file.  We will also upload all files to the artifacts store for documentation purposes.
        # and return the spec for further processing.
        if name in spec_mapping:
            log.info("Loading spec name={}", name)
            process_func, task = spec_mapping[name]
            data = process_func(zipfile_obj.read(name))
            specs[task] = DeploySpec(actions=data)

        # Upload all files to the artifacts store for documentation purposes
        # and this includes the CloudFormation templates needed for the actions.

        key = dd.get_artefacts_key(name)
        data = zipfile_obj.read(name)

        log.info("Uploading file: {})", key)

        bucket.put_object(Key=key, Body=data, ServerSideEncryption="AES256")

    # Process deployspec
    if not specs:
        raise Exception("Package does not contain any deployspec files, cannot continue")

    return specs


def get_accounts_regions(action_spec: ActionSpec) -> tuple[list[str], list[str]]:
    """
    Compile a list of accounts and regions for the action.

    We will combine the fields `account` and `accounts` into a single list.
    We will combine the fields `region` and `regions` into a single list.

    If no region is specified, then the default region will be used.

    :param action_spec: The action specification to extract accounts and regions from
    :type action_spec: ActionSpec
    :returns: Tuple containing lists of accounts and regions
    :rtype: tuple[list[str], list[str]]

    Examples
    --------
    >>> action_spec = ActionSpec(
    ...     params={"account": "123456789012", "region": "us-east-1"}
    ... )
    >>> accounts, regions = get_accounts_regions(action_spec)
    >>> # Returns: (["123456789012"], ["us-east-1"])
    """
    accounts = action_spec.params.get("accounts") or action_spec.params.get("Accounts") or []
    account = action_spec.params.get("account") or action_spec.params.get("Account")
    if account and account not in accounts:
        accounts.append(account)

    regions = action_spec.params.get("regions") or action_spec.params.get("Regions") or []
    region = action_spec.params.get("region") or action_spec.params.get("Region") or util.get_region()
    if region and region not in regions:
        regions.append(region)

    return accounts, regions


def get_region_account_labels(action_spec: ActionSpec) -> list[str]:
    """
    Generate a unique list of labels for the action specification
    for each account/region permutation.

    :param action_spec: The action specification to generate labels for
    :type action_spec: ActionSpec
    :returns: List of generated labels for account/region combinations
    :rtype: list[str]

    Examples
    --------
    >>> action_spec = ActionSpec(
    ...     label="create-vpc",
    ...     params={"accounts": ["123", "456"], "regions": ["us-east-1", "us-west-2"]}
    ... )
    >>> labels = get_region_account_labels(action_spec)
    >>> # Returns: ["create-vpc-123-us-east-1", "create-vpc-123-us-west-2", 
    >>> #           "create-vpc-456-us-east-1", "create-vpc-456-us-west-2"]
    """
    accounts, regions = get_accounts_regions(action_spec)

    labels = [f"{action_spec.label}-{account}-{region}" for account in accounts for region in regions]

    return labels


def compile_deployspec(task_payload: TaskPayload, deployspec: DeploySpec) -> list[ActionSpec]:
    """
    Convert deployspec into an actions list.

    :param task_payload: The task payload containing deployment context
    :type task_payload: TaskPayload
    :param deployspec: The deployspec to compile into actions
    :type deployspec: DeploySpec
    :returns: List of compiled action specifications
    :rtype: list[ActionSpec]
    :raises ValueError: If unknown action type is encountered

    Examples
    --------
    >>> deployspec = DeploySpec(actions=[
    ...     ActionSpec(type="create_stack", label="vpc", params={...})
    ... ])
    >>> actions = compile_deployspec(task_payload, deployspec)
    >>> # Returns: [ActionSpec(...)]
    """
    spec_label_map: SpecLabelMapType = {}
    for action_spec in deployspec.action_specs:
        spec_label_map[action_spec.label] = get_region_account_labels(action_spec)

    log.debug("spec_label_map", details=spec_label_map)

    # Catalog of known Deployspec types
    routes: dict[str, dict] = {
        "create_stack": {"allow_multiple_stacks": True, "kind": CreateStackActionSpec},
        "delete_stack": {"allow_multiple_stacks": True, "kind": DeleteStackActionSpec},
        "create_user": {"allow_multiple_stacks": False, "kind": PutUserActionSpec},
        "delete_user": {"allow_multiple_stacks": False, "kind": DeleteUserActionSpec},
        "create_change_set": {"allow_multiple_stacks": False, "kind": CreateChangeSetActionSpec},
        "apply_change_set": {"allow_multiple_stacks": False, "kind": ApplyChangeSetActionSpec},
        "delete_change_set": {"allow_multiple_stacks": False, "kind": DeleteChangeSetActionSpec},
    }

    # For the actions specified in the deployspec, compile them into a list of actions for the core_execute module
    compiled_actions: list[ActionSpec] = []
    for action_spec in deployspec.action_specs:
        params = routes.get(action_spec.type, None)
        if not params:
            raise ValueError(f"Unknown action type {action_spec.type}")
        compiled_actions.extend(compile_action(action_spec, task_payload, spec_label_map, **params))
    return compiled_actions


def compile_action(
    action_spec: ActionSpec, task_payload: TaskPayload, spec_label_map: SpecLabelMapType, **kwargs
) -> list[ActionSpec]:
    """
    Compile a single action specification into executable actions.

    :param action_spec: The action specification to compile
    :type action_spec: ActionSpec
    :param task_payload: The task payload containing deployment context
    :type task_payload: TaskPayload
    :param spec_label_map: Mapping of spec labels to region/account combinations
    :type spec_label_map: SpecLabelMapType
    :param kwargs: Additional parameters including allow_multiple_stacks and kind
    :type kwargs: dict
    :returns: List of compiled action specifications
    :rtype: list[ActionSpec]
    :raises ValueError: If required account/region information is missing or invalid

    Examples
    --------
    >>> action_spec = ActionSpec(type="create_stack", label="vpc", params={...})
    >>> actions = compile_action(action_spec, task_payload, spec_label_map, 
    ...                         allow_multiple_stacks=True, kind=CreateStackActionSpec)
    >>> # Returns: [ActionSpec(...)]
    """
    accounts, regions = get_accounts_regions(action_spec)

    allow_multiple_stacks = kwargs.get("allow_multiple_stacks", False)
    klass = kwargs.get("kind", None)

    if not allow_multiple_stacks:
        if len(accounts) == 0 or len(regions) == 0:
            raise ValueError("Missing account or region")

        if len(accounts) > 1 or len(regions) > 1:
            raise ValueError(f"Cannot {action_spec.type} from multiple accounts or regions")

    action_list: list[ActionSpec] = []
    for account in accounts:
        for region in regions:
            action_list.append(generate_action_command(task_payload, action_spec, spec_label_map, account, region))
    return action_list


def generate_action_command(
    task_payload: TaskPayload,
    action_spec: ActionSpec,
    spec_label_map: SpecLabelMapType,
    account: str,
    region: str,
) -> ActionSpec:
    """
    Generate an executable action command from an action specification.

    :param task_payload: The task payload containing deployment context
    :type task_payload: TaskPayload
    :param action_spec: The action specification to generate command for
    :type action_spec: ActionSpec
    :param spec_label_map: Mapping of spec labels to region/account combinations
    :type spec_label_map: SpecLabelMapType
    :param account: The AWS account ID for this action
    :type account: str
    :param region: The AWS region for this action
    :type region: str
    :returns: The generated action specification
    :rtype: ActionSpec
    :raises ValueError: If action type is required but not provided

    Examples
    --------
    >>> action_spec = ActionSpec(action="AWS::CreateStack", label="vpc", params={...})
    >>> command = generate_action_command(task_payload, action_spec, spec_label_map, 
    ...                                  "123456789012", "us-east-1")
    >>> # Returns: ActionSpec with executable parameters
    """

    if action_spec.action is None:
        raise ValueError("Action type is required that matches a valid Action model")

    deployment_details = task_payload.deployment_details
    package_details = task_payload.package
    bucket_name = package_details.bucket_name
    bucket_region = package_details.bucket_region
    label = f"{action_spec.label}-{account}-{region}"
    depends_on = __get_depends_on(action_spec, spec_label_map)
    scope = __get_action_scope(action_spec, deployment_details)
    klass = action_spec.kind

    # These are the minimum required fields.
    execute_action = {
        "Label": label,
        "Kind": action_spec.kind,
        "DependsOn": depends_on,
        "Params": {
            "Account": account,
            "Region": region,
        },
        "Scope": scope,
    }

    # Perform the following actions if the action_spec is PutUser
    user_name = action_spec.params.get("user_name") or action_spec.params.get("UserName")
    if user_name:
        execute_action["Params"]["UserName"] = user_name

    # Perform the following actions if the action_spec is a CloudFormation action
    if action_spec.action == "AWS::CreateStack":
        stack_name = action_spec.params.get("stack_name") or action_spec.params.get("StackName")
        if stack_name:
            execute_action["Params"]["StackName"] = stack_name
        stack_parameters = __apply_syntax_update(action_spec.params.parameters)
        if stack_parameters:
            execute_action["Params"]["StackParameters"] = stack_parameters
        stack_policy = action_spec.params.get("stack_policy") or action_spec.params.get("StackPolicy")
        if stack_policy:
            # If the stack policy is a string, we will assume it's a JSON string.
            # If it's a dict, we will convert it to a JSON string.
            # If it's None, we will not include it in the action.
            execute_action["Params"]["StackPolicy"] = __get_stack_policy_json(stack_policy)
        template_url = get_action_template_url(action_spec, bucket_name, bucket_region, deployment_details)
        if template_url:
            execute_action["Params"]["TemplateUrl"] = template_url
        tags = __get_tags(scope, deployment_details)
        if tags:
            execute_action["Params"]["Tags"] = tags

    return execute_action


def get_action_template_url(
    action_spec: ActionSpec,
    bucket_name: str,
    bucket_region: str,
    deployment_details: DeploymentDetails,
) -> str | None:
    """
    Get the template URL for a CloudFormation action.

    :param action_spec: The action specification containing template parameters
    :type action_spec: ActionSpec
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
    >>> action_spec = ActionSpec(params={"template_url": "vpc.yaml"})
    >>> url = get_action_template_url(action_spec, "my-bucket", "us-east-1", deployment_details)
    >>> # Returns: "s3://my-bucket/artifacts/vpc.yaml"
    """

    key = action_spec.params.get("template_url") or action_spec.params.get("TemplateUrl")
    if key is None:
        return None
    scope = __get_action_scope(action_spec, deployment_details)

    return get_template_url(bucket_name, bucket_region, deployment_details, key, scope)


def get_template_url(
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
    >>> url = get_template_url("my-bucket", "us-east-1", deployment_details, 
    ...                       "vpc.yaml", "build")
    >>> # Returns: "s3://my-bucket/artifacts/client/portfolio/app/branch/build/vpc.yaml"
    """

    if not template:
        template = ""

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


def apply_context(actions: list[ActionSpec], context: dict) -> list[ActionSpec]:
    """
    Apply state to the actions list. Uses Jinja Template to render the state.

    :param actions: The list of action specifications to render
    :type actions: list[ActionSpec]
    :param context: The context dictionary for template rendering
    :type context: dict
    :returns: The rendered actions list with context applied
    :rtype: list[ActionSpec]
    :raises ValueError: If unknown action type is encountered in actions list

    Examples
    --------
    >>> actions = [ActionSpec(params={"StackName": "{{ core.portfolio }}-vpc"})]
    >>> context = {"core": {"portfolio": "web-services"}}
    >>> rendered = apply_context(actions, context)
    >>> # Returns: [ActionSpec(params={"StackName": "web-services-vpc"})]
    """

    actions_list: list[dict[str, Any]] = [a.model_dump() for a in actions]

    unrendered_contents = util.to_yaml(actions_list)

    # Create a Jinja2 template from the deployspec contents
    template = Template(unrendered_contents)

    # Render the template with the provided state.  I don't know
    # if the root should be "core" or if the root should be "context".
    # If we change the root to "context", then we need to change the
    # input to template.render(context=context[CONTEXT_ROOT])
    rendered_contents = template.render(core=context[CONTEXT_ROOT])

    action_list = util.from_yaml(rendered_contents)

    # Convert the action list to ActionSpec objects
    # Please note that the value of action.kind and action.params is NOT validated here.
    actions: list[ActionSpec] = []
    for action in action_list:
        if isinstance(action, dict):
            actions.append(ActionSpec(**action))
        elif isinstance(action, ActionSpec):
            actions.append(action)
        else:
            raise ValueError(f"Unknown action type {type(action)} in actions list")

    log.debug("Compiled actions: {}", actions)

    # Return the rendered actions list
    return actions


def __get_stack_policy_json(stack_policy: str | dict | None) -> str | None:
    """
    Convert stack policy to JSON string format if defined.

    :param stack_policy: The input policy as string, dict, or None
    :type stack_policy: str | dict | None
    :returns: Policy statement in JSON format or None
    :rtype: str | None

    Examples
    --------
    >>> policy = {"Statement": [{"Effect": "Allow", "Principal": "*", "Action": "*"}]}
    >>> json_policy = __get_stack_policy_json(policy)
    >>> # Returns: '{"Statement": [{"Effect": "Allow", "Principal": "*", "Action": "*"}]}'
    """
    if not stack_policy:
        return None
    if isinstance(stack_policy, dict) or isinstance(stack_policy, str):
        return util.to_json(stack_policy)
    else:
        return None


def __get_tags(scope: str | None, deployment_details: DeploymentDetails) -> dict | None:
    """
    Generate AWS tags based on deployment scope and details.

    :param scope: The deployment scope (portfolio, app, branch, build)
    :type scope: str | None
    :param deployment_details: The deployment details containing tag information
    :type deployment_details: DeploymentDetails
    :returns: Dictionary of AWS tags or None if no tags
    :rtype: dict | None

    Examples
    --------
    >>> tags = __get_tags("build", deployment_details)
    >>> # Returns: {"Portfolio": "core", "App": "api", "Branch": "master", "Build": "1234"}
    """

    if not scope or scope == V_EMPTY:
        scope = SCOPE_BUILD

    tags: dict[str, str] = deployment_details.tags or {}

    if deployment_details.portfolio:
        tags[TAG_PORTFOLIO] = deployment_details.portfolio

    if scope in [SCOPE_APP, SCOPE_BRANCH, SCOPE_BUILD] and deployment_details.app:
        tags[TAG_APP] = deployment_details.app

    if scope in [SCOPE_BRANCH, SCOPE_BUILD] and deployment_details.branch:
        tags[TAG_BRANCH] = deployment_details.branch

    if scope == SCOPE_BUILD and deployment_details.build:
        tags[TAG_BUILD] = deployment_details.build

    return tags if len(tags) > 0 else None


def __apply_syntax_update(stack_parameters: dict | None) -> dict | None:
    """
    Deal with runner syntax changes for backward compatibility.
    
    Converts old syntax: ``S3ComplianceBucketName: "{{ foo.bar }}"``
    To new syntax: ``S3ComplianceBucketName: "{{ 'foo/bar' | lookup }}"``

    :param stack_parameters: The stack parameters dictionary to update
    :type stack_parameters: dict | None
    :returns: The updated stack parameters with new syntax
    :rtype: dict | None

    Examples
    --------
    >>> params = {"BucketName": "{{ portfolio.name }}"}
    >>> updated = __apply_syntax_update(params)
    >>> # Returns: {"BucketName": "{{ 'portfolio/name' | lookup }}"}
    """
    if not stack_parameters:
        return None

    # NOTE:  See this is expecting "core" to be the root.
    # See the "apply_context" function.  It's not clear if the root should be "core" or "context".
    for key in stack_parameters:
        stack_parameters[key] = re.sub(
            r"{{ (?!core.)([^.]*)\.(.*) }}",
            r"{{ '\1/\2' | lookup }}",
            "{}".format(stack_parameters[key]),  # Must be a string for re.sub to work, so fails when your parameter is a number.
        )
    return stack_parameters


def __get_depends_on(action: ActionSpec, spec_label_map: SpecLabelMapType) -> list:
    """
    Get the dependency list for an action specification.

    :param action: The action specification to get dependencies for
    :type action: ActionSpec
    :param spec_label_map: Mapping of spec labels to region/account combinations
    :type spec_label_map: SpecLabelMapType
    :returns: List of action labels this action depends on
    :rtype: list

    Examples
    --------
    >>> action = ActionSpec(depends_on=["vpc", "security"])
    >>> deps = __get_depends_on(action, spec_label_map)
    >>> # Returns: ["vpc-123-us-east-1", "security-123-us-east-1"]
    """

    if not action.depends_on:
        return []

    depends_on: list = [item for sublist in map(lambda label: spec_label_map[label], action.depends_on) for item in sublist]

    return depends_on


def __get_action_scope(action: ActionSpec, deployment_details: DeploymentDetails) -> str:
    """
    Determine the deployment scope for an action based on stack name templates.

    Relies on the deployspec to have templating to determine the scope or you can specify 
    the scope in the action object.

    Example stack_name: ``"{{ core.Project }}-{{ core.App }}-resources"``
    The above will return the scope of SCOPE_APP ('app').

    :param action: The action specification to determine scope for
    :type action: ActionSpec
    :param deployment_details: The deployment details containing scope information
    :type deployment_details: DeploymentDetails
    :returns: The deployment scope (portfolio, app, branch, build)
    :rtype: str

    Examples
    --------
    >>> action = ActionSpec(params={"stack_name": "{{ core.Portfolio }}-{{ core.App }}-vpc"})
    >>> scope = __get_action_scope(action, deployment_details)
    >>> # Returns: "app"
    """

    if action.scope:
        return action.scope

    if deployment_details.scope:
        return deployment_details.scope

    stack_name = action.params.get("stack_name") or action.params.get("StackName") or ""

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

"""Description: Compile a deployspec package into actions and templates.

- Extracts package files to a location in S3
- Parses and compiles the package deployspec (deployspec.yml)
- Uploads actions to S3
"""

from typing import Any
from types import SimpleNamespace
import io
import os
import re
import zipfile as zip
from ruamel import yaml
import json
import core_logging as log

import core_framework as util
import core_helper.aws as aws

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
    V_DEPLOYSPEC_FILE_YAML,
    V_DEPLOYSPEC_FILE_YML,
    V_DEPLOYSPEC_FILE_JSON,
    V_EMPTY,
    V_PACKAGE_ZIP,
)

from core_framework.models import (
    ActionDefinition,
    ActionParams,
    TaskPayload,
    DeploySpec,
    ActionSpec,
    DeploymentDetails,
    PackageDetails,
)

SpecLabelMapType = dict[str, list[str]]


class LocalBucket:

    Name: str | None
    Key: str | None
    Body: str | None
    ServerSideEncryption: str | None

    def __init__(self, name: str, app_dir: str):
        self.Name = name
        self.app_dir = app_dir

    def download_fileobj(self, fileobj: io.BytesIO, key: str):
        self.Key = key
        with open(self.Key, "rb") as file:
            fileobj.write(file.read())
        fileobj.seek(0)

    def put_object(self, **kwargs) -> SimpleNamespace:

        if not self.app_dir:
            return SimpleNamespace(version_id="local")

        destination = kwargs.get("Key")
        if not destination:
            return SimpleNamespace(version_id="local")

        body = kwargs.get("Body")
        if not body:
            return SimpleNamespace(version_id="local")
        # encryption = kwargs["ServerSideEncryption"]
        # acl = kwargs.get("ACL")

        fn = os.path.join(self.app_dir, destination)

        dirname = os.path.dirname(fn)
        os.makedirs(dirname, exist_ok=True)

        with open(fn, "wb") as file:
            file.write(body)

        return SimpleNamespace(version_id="local")


def process_package_local(
    package_details: PackageDetails, upload_prefix: str = ""
) -> DeploySpec:
    """
    Process package For local mode.

    Args:
        package_details (PackageDetails): The package details haveing the location of the deployspec.

    Returns:
        DeploySpec: The deployspec object
    """
    app_dir = package_details.AppPath

    if not app_dir:
        app_dir = os.getcwd()

    pkg = (
        os.path.join(app_dir, package_details.Key) if package_details.Key else app_dir
    )

    log.debug("Loading local files for app_dir={}".format(app_dir))

    if not pkg.endswith(V_PACKAGE_ZIP):
        pkg = os.path.join(pkg, V_PACKAGE_ZIP)

    # If there is a packge.zip file in this folder, we can process it.
    if os.path.exists(pkg):

        # Initialize a local bucket with the "root" path and load the package.zip file
        zip_fileobj = io.BytesIO()
        bucket = LocalBucket(package_details.BucketName, app_dir)
        bucket.download_fileobj(zip_fileobj, pkg)

        # Process the zip file
        return process_package_zip(zip_fileobj, bucket, upload_prefix, os.path.sep)

    # If there isn't a package.zip in the app_dir, then we will look for a deployspec file.
    data = util.common.load_deployspec(app_dir)
    if not data:
        raise Exception("Package does not contain a deployspec file, cannot continue")

    package_details.DeploySpec = DeploySpec(actions=data)

    return package_details.DeploySpec


def process_package_s3(
    package_details: PackageDetails, upload_prefix: str = ""
) -> DeploySpec:
    """
    Read the contest of the BytesIO buffer which should contain a zip file.

    s3_bucket: The S3 bucket to upload the files to.

        ```python
        s3 = aws.s3_resource(bucket_region)
        bucket = s3.Bucket(bucket_name)
        ```

    Args:
        package_details (PackageDetails): The package details having the location of the package.zip
        upload_prefix (str): Uploading all files to S3 artefacts location

    Raises:
        Exception: if a deployspec file is not found in the zip file

    Returns:
        DeploySpec: Object representing the deployspec

    """

    bucket_name = package_details.BucketName
    bucket_region = package_details.BucketRegion
    package_key = package_details.Key

    # Download file from S3
    s3 = aws.s3_resource(bucket_region)
    log.info(
        "Downloading package from S3 (bucket: {}, key: {})".format(
            bucket_name, package_key
        )
    )

    # Read the file io stream from AWS S3
    zip_fileobj = io.BytesIO()
    bucket = s3.Bucket(bucket_name)
    bucket.download_fileobj(package_key, zip_fileobj)

    # Process the zip file
    return process_package_zip(zip_fileobj, bucket, upload_prefix)


def process_package_zip(
    zip_fileobj: io.BytesIO, bucket: Any, upload_prefix: str = "", sep: str = "/"
) -> DeploySpec:

    # Note that we have not yet seen a deployspc file
    deployspec: DeploySpec | None = None

    zipfile = zip.ZipFile(zip_fileobj, "r")

    log.debug(
        "Extracting {} and Uploading artefact to: {}", V_PACKAGE_ZIP, upload_prefix
    )

    for name in zipfile.namelist():

        if name == V_DEPLOYSPEC_FILE_YML or name == V_DEPLOYSPEC_FILE_YAML:

            log.info("Loading deployspec name={}", name)
            y = yaml.YAML(typ="safe")
            data = y.load(zipfile.read(name))
            deployspec = DeploySpec(actions=data)

        elif name == V_DEPLOYSPEC_FILE_JSON:

            log.info("Loading deployspec name={}", name)

            data = json.loads(zipfile.read(name))
            deployspec = DeploySpec(actions=data)

        else:
            # Upload CFN templates etc to S3 now.
            key = f"{upload_prefix}{sep}{name}"

            log.debug("Uploading file: {})", key)

            bucket.put_object(
                Key=key, Body=zipfile.read(name), ServerSideEncryption="AES256"
            )

    # Process deployspec
    if not deployspec:
        raise Exception("Package does not contain a deployspec file, cannot continue")

    return deployspec


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
    deployment_details: DeploymentDetails,
    key: str | None = None,
    scope: str | None = None,
) -> str:

    if not key:
        key = ""

    return "https://s3-{}.amazonaws.com/{}/{}".format(
        bucket_region,
        bucket_name,
        util.get_artefact_key(deployment_details, key, scope),
    )


def upload_actions(
    bucket: Any, upload_prefix: str, actions_string: str, sep: str = "/"
) -> tuple[str, str]:
    """
    Upload actions to S3.

    Args:
        bucket (Any): The S3 bucket boto3 resource.
        upload_prefix (str): The upload prefix path
        actions_string (str): The actions file contents.

    """
    actions_key = f"{upload_prefix}{sep}deploy.actions"

    print("DEBUG: Uploading file to S3 automation bucket (key: {})".format(actions_key))

    object = bucket.put_object(
        Body=actions_string.encode("utf-8"),
        Key=actions_key,
        ServerSideEncryption="AES256",
        ACL="bucket-owner-full-control",
    )
    actions_version = object.version_id

    return actions_key, actions_version


def apply_state(deployspec_contents: str, state: dict) -> str:
    """
    Apply state to the deployspec contents.  Uses Jinja Template to render the state.

    Args:
        deployspec_contents (str): The deployspec file contents.
        state (dict): The state to apply to the deployspec.

    Returns:
        str: The deployspec file contents with the state modifiecations applied
    """
    # Create a Jinja2 template from the deployspec contents
    template = Template(deployspec_contents)

    # Render the template with the provided state
    rendered_contents = template.render(core=state["core"])

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


def to_yaml(data: list[dict]) -> str:
    """
    Dump to a yaml string with some sane defaults.

    Args:
        data (dict): The data to dump to yaml.

    Returns:
        str: The yaml string.

    """
    y = yaml.YAML(typ="rt")
    y.default_flow_style = False
    y.allow_unicode = True
    y.indent(mapping=2, sequence=2, offset=0)

    s = io.StringIO()
    y.dump(data, s)

    return s.getvalue()

"""
Test data setup utilities for deployspec testing.

This module provides functions to bootstrap DynamoDB tables and create test data
for clients, portfolios, zones, and applications needed for deployspec testing.
"""

from typing import Any

import core_framework as util
import core_helper.aws as aws

from core_db.event import EventModel
from core_db.item import ItemModel
from core_db.registry.client import ClientFacts
from core_db.registry.portfolio import (
    PortfolioFacts,
    ContactFacts,
    ApproverFacts,
    ProjectFacts,
    OwnerFacts,
)
from core_db.registry.app import AppFacts
from core_db.registry.zone import (
    ZoneFacts,
    ZoneFacts,
    AccountFacts as AccountFactsModel,
    RegionFacts as RegionFactsModel,
    KmsFacts,
    SecurityAliasFacts,
    ProxyFacts,
)


def bootstrap_dynamo() -> bool:
    """
    Bootstrap DynamoDB tables for testing.

    Creates all required DynamoDB tables if they don't exist.
    Assumes local DynamoDB is running on localhost:8000.

    :returns: True if bootstrap successful, raises assertion error otherwise
    :rtype: bool
    :raises AssertionError: If DynamoDB host is not localhost:8000 or table creation fails

    Examples
    --------
    >>> success = bootstrap_dynamo()
    >>> assert success == True
    """
    # see environment variables in .env
    host = util.get_dynamodb_host()

    assert host == "http://localhost:8000", "DYNAMODB_HOST must be set to http://localhost:8000"

    try:
        if not EventModel.exists():
            EventModel.create_table(wait=True)

        if not ItemModel.exists():
            ItemModel.create_table(wait=True)

        if not ClientFacts.exists():
            ClientFacts.create_table(wait=True)

        if not PortfolioFacts.exists():
            PortfolioFacts.create_table(wait=True)

        if not AppFacts.exists():
            AppFacts.create_table(wait=True)

        # Fixed: Check if ZoneFacts exists before trying to delete
        if ZoneFacts.exists():
            ZoneFacts.delete_table()

        if not ZoneFacts.exists():
            ZoneFacts.create_table(wait=True)

    except Exception as e:
        print(f"Error bootstrapping DynamoDB: {e}")  # Fixed: use f-string
        assert False, f"Failed to bootstrap DynamoDB: {e}"  # Fixed: provide error message

    return True


def get_organization() -> dict[str, str]:
    """
    Return organization information for the AWS Profile.

    Retrieves organization details from AWS Organizations service
    including ID, master account ID, name, and email.

    :returns: Dictionary containing organization information
    :rtype: dict[str, str]

    Examples
    --------
    >>> org_info = get_organization()
    >>> # Returns: {"id": "o-1234567890", "account_id": "123456789012", ...}
    """
    organization: dict[str, str] = {"id": "", "account_id": "", "name": "", "email": ""}

    try:
        oc = aws.org_client()
        orginfo = oc.describe_organization()
        org = orginfo.get("Organization", {})
        organization.update(
            {
                "id": org.get("Id", ""),
                "account_id": org.get("MasterAccountId", ""),
                "email": org.get("MasterAccountEmail", ""),
            }
        )

        if organization["account_id"]:
            response = oc.describe_account(AccountId=organization["account_id"])
            organization["name"] = response.get("Account", {}).get("Name", "")

    except Exception:  # pylint: disable=broad-except
        pass

    return organization


def get_client_data(organization: dict[str, str], arguments: dict[str, Any]) -> ClientFacts:
    """
    Create and save ClientFacts test data.

    :param organization: Organization information dictionary
    :type organization: dict[str, str]
    :param arguments: Arguments containing client name
    :type arguments: dict[str, Any]
    :returns: Created ClientFacts instance
    :rtype: ClientFacts
    :raises AssertionError: If 'client' key is not in arguments

    Examples
    --------
    >>> org_data = {"account_id": "123456789012", "id": "o-1234567890"}
    >>> args = {"client": "acme"}
    >>> client_facts = get_client_data(org_data, args)
    """
    assert "client" in arguments, "Client name must be provided in arguments"

    client = arguments["client"]

    region = util.get_region()
    bucket_name = util.get_bucket_name()

    aws_account_id = organization["account_id"]

    cf = ClientFacts(
        client=client,
        domain="my-domain.com",
        organization_id=organization["id"],
        organization_name=organization["name"],
        organization_account=organization["account_id"],
        organization_email=organization["email"],
        client_region=region,
        master_region=region,
        automation_account=aws_account_id,
        bucket_name=bucket_name,
        bucket_region=region,
        audit_account=aws_account_id,
        docs_bucket_name=bucket_name,
        security_account=aws_account_id,
        ui_bucket=bucket_name,
        scope="",
    )
    cf.save()

    return cf


def get_portfolio_data(client_data: ClientFacts, arguments: dict[str, Any]) -> PortfolioFacts:
    """
    Create and save PortfolioFacts test data.

    :param client_data: ClientFacts instance containing client information
    :type client_data: ClientFacts
    :param arguments: Arguments containing portfolio name
    :type arguments: dict[str, Any]
    :returns: Created PortfolioFacts instance
    :rtype: PortfolioFacts
    :raises AssertionError: If 'portfolio' key is not in arguments

    Examples
    --------
    >>> args = {"portfolio": "core"}
    >>> portfolio_facts = get_portfolio_data(client_facts, args)
    """
    assert "portfolio" in arguments, "Portfolio name must be provided in arguments"

    portfolio_name = arguments["portfolio"]

    # Fixed: Use consistent attribute access - use lowercase attributes
    domain_name = client_data.Domain  # Fixed: use lowercase
    client = client_data.Client  # Fixed: use lowercase

    portfolio = PortfolioFacts(
        client=client,  # Fixed: use lowercase field names
        portfolio=portfolio_name,  # Fixed: use lowercase field names
        contacts=[ContactFacts(name="John Doe", email="john.doe@example.com")],  # Fixed: email domain
        approvers=[ApproverFacts(name="Jane Doe", email="jane.doe@example.com", roles=["admin"], sequence=1)],  # Fixed: email
        project=ProjectFacts(name="my-project", description="my project description", code="MYPRJ"),
        bizapp=ProjectFacts(name="my-bizapp", description="my bizapp description", code="MYBIZ"),
        owner=OwnerFacts(name="John Doe", email="john.doe@example.com"),  # Fixed: email domain
        domain=f"my-app.{domain_name}",
        tags={
            "BizApp": "MyBizApp",  # Fixed: typo in "BizzApp"
            "Manager": "John Doe",
        },
        metadata={
            "misc": "items",
            "date": "2021-01-01",
        },
    )
    portfolio.save()

    return portfolio


def get_zone_data(client_data: ClientFacts, arguments: dict[str, Any]) -> ZoneFacts:
    """
    Create and save ZoneFacts test data.

    :param client_data: ClientFacts instance containing client information
    :type client_data: ClientFacts
    :param arguments: Arguments dictionary (unused but kept for consistency)
    :type arguments: dict[str, Any]
    :returns: Created ZoneFacts instance
    :rtype: ZoneFacts

    Examples
    --------
    >>> zone_facts = get_zone_data(client_facts, {})
    """
    # Fixed: Use lowercase attribute access
    automation_account_id = client_data.AutomationAccount
    automation_account_name = client_data.OrganizationAccount

    zone = ZoneFacts(
        client=client_data.Client,  # Fixed: use lowercase field names
        zone="my-automation-service-zone",
        account_facts=AccountFactsModel(  # Fixed: use lowercase field names
            client=client_data.Client,
            aws_account_id=automation_account_id,
            organizational_unit="PrimaryUnit",
            account_name=automation_account_name,
            environment="prod",
            kms=KmsFacts(
                aws_account_id=automation_account_id,
                kms_key_arn="arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
                kms_key="alias/my-kms-key",
                delegate_aws_account_ids=[automation_account_id],
            ),
            resource_namespace="my-automation-service",
            network_name="my-network-from-ciscos",
            vpc_aliases={
                "primary-network": "my-cisco-network-primary-network-id",
                "secondary-network": "my-cisco-network-secondary-network-id",
            },
            subnet_aliases={
                "ingress": "my-cisco-network-ingress-subnet-id",
                "workload": "my-cisco-network-workload-subnet-id",
                "egress": "my-cisco-network-egress-subnet-id",
            },
            tags={"Zone": "my-automation-service-zone"},
        ),
        region_facts={
            "sin": RegionFactsModel(
                aws_region="ap-southeast-1",
                az_count=3,
                image_aliases={"imageid:latest": "ami-2342342342344"},
                min_successful_instances_percent=100,
                security_aliases={
                    "global_cidrs": [
                        SecurityAliasFacts(
                            type="cidr",
                            value="192.168.0.0/16",
                            description="Global CIDR 1",
                        ),
                        SecurityAliasFacts(type="cidr", value="10.0.0.0/8", description="Global CIDR 2"),
                    ]
                },
                security_group_aliases={
                    "alias1": "aws_sg_ingress",
                    "alias2": "aws-sg-egress-groups",
                },
                proxy=[
                    ProxyFacts(
                        host="myproxy.proxy.com",  # Fixed: typo "myprox"
                        port=8080,
                        url="http://proxy.acme.com:8080",
                        no_proxy="10.0.0.0/8,192.168.0.0/16,*.acme.com",
                    )
                ],
                proxy_host="myproxy.proxy.com",  # Fixed: typo "myprox"
                proxy_port=8080,
                proxy_url="http://proxy.acme.com:8080",
                no_proxy="127.0.0.1,localhost,*.acme.com",
                name_servers=["192.168.1.1"],
                tags={"Region": "sin"},
            )
        },
        tags={"Zone": "my-automation-service-zone"},
    )
    zone.save()

    return zone


def get_app_data(portfolio_data: PortfolioFacts, zone_data: ZoneFacts, arguments: dict[str, Any]) -> AppFacts:
    """
    Create and save AppFacts test data.

    :param portfolio_data: PortfolioFacts instance containing portfolio information
    :type portfolio_data: PortfolioFacts
    :param zone_data: ZoneFacts instance containing zone information
    :type zone_data: ZoneFacts
    :param arguments: Arguments containing app name
    :type arguments: dict[str, Any]
    :returns: Created AppFacts instance
    :rtype: AppFacts
    :raises AssertionError: If 'app' key is not in arguments

    Examples
    --------
    >>> args = {"app": "api"}
    >>> app_facts = get_app_data(portfolio_facts, zone_facts, args)
    """
    assert "app" in arguments, "App name must be provided in arguments"

    # The client/portfolio is where this BizApp that this Deployment is for.
    # The Zone is where this BizApp component will be deployed.

    # Fixed: Use lowercase attribute access
    client = portfolio_data.Client
    portfolio = portfolio_data.Portfolio
    app = arguments["app"]

    client_portfolio_key = f"{client}:{portfolio}"

    app_facts = AppFacts(  # Fixed: variable name conflict
        client_portfolio=client_portfolio_key,  # Fixed: use lowercase field names
        app_regex=f"^prn:{portfolio}:{app}:.*:.*$",
        zone=zone_data.Zone,
        name="test application",
        environment="prod",
        image_aliases={"image1": "awsImageID1234234234"},
        repository="https://github.com/my-org/my-portfolio-my-app.git",
        region="sin",
        tags={"Disposition": "Testing"},
        metadata={"misc": "items"},
    )

    app_facts.save()

    return app_facts


def initialize(arguments: dict[str, Any]) -> tuple[ClientFacts, ZoneFacts, PortfolioFacts, AppFacts]:
    """
    Initialize all test data for deployspec testing.

    Creates all required test data including client, zone, portfolio, and app facts.

    :param arguments: Arguments containing client, portfolio, and app names
    :type arguments: dict[str, Any]
    :returns: Tuple of created test data instances
    :rtype: tuple[ClientFacts, ZoneFacts, PortfolioFacts, AppFacts]
    :raises Exception: If DynamoDB bootstrap fails

    Examples
    --------
    >>> args = {"client": "acme", "portfolio": "core", "app": "api"}
    >>> client_data, zone_data, portfolio_data, app_data = initialize(args)
    """
    if not bootstrap_dynamo():
        raise Exception("Failed to bootstrap DynamoDB")

    org_data = get_organization()

    client_data: ClientFacts = get_client_data(org_data, arguments)
    zone_data: ZoneFacts = get_zone_data(client_data, arguments)
    portfolio_data: PortfolioFacts = get_portfolio_data(client_data, arguments)
    app_data: AppFacts = get_app_data(portfolio_data, zone_data, arguments)

    return client_data, zone_data, portfolio_data, app_data

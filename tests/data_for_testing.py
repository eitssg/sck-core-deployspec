"""
Test data setup utilities for deployspec testing.

This module provides functions to bootstrap DynamoDB tables and create test data
for clients, portfolios, zones, and applications needed for deployspec testing.
"""

import pytest
from typing import Any

import core_logging as log
import core_framework as util
import core_helper.aws as aws

from core_db.registry.client import ClientFactsModel, ClientFactsFactory
from core_db.registry.app import AppFactsModel, AppFactsFactory
from core_db.registry.portfolio import (
    PortfolioFactsModel,
    ContactFacts,
    ApproverFacts,
    ProjectFacts,
    OwnerFacts,
    PortfolioFactsFactory,
)
from core_db.registry.zone import (
    ZoneFactsModel,
    ZoneFactsFactory,
    AccountFacts,
    RegionFacts,
    KmsFacts,
    SecurityAliasFacts,
    ProxyFacts,
)

from .bootstrap import *


def get_organization(real_aws: bool) -> dict[str, str]:
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
    organization: dict[str, str] = {
        "id": "o-t73gu32ai5",
        "account_id": "154798051514",
        "name": "eits-billing",
        "email": "jbarwick@eits.com.sg",
    }

    if real_aws:
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


def get_client_data(organization: dict[str, str], arguments: dict[str, Any]) -> ClientFactsModel:
    """
    Create and save ClientFactsModel test data.

    :param organization: Organization information dictionary
    :type organization: dict[str, str]
    :param arguments: Arguments containing client name
    :type arguments: dict[str, Any]
    :returns: Created ClientFactsModel instance
    :rtype: ClientFactsModel
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

    model = ClientFactsFactory.get_model()
    cf = model(
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


def get_portfolio_data(client_data: ClientFactsModel, arguments: dict[str, Any]) -> PortfolioFactsModel:
    """
    Create and save PortfolioFactsModel test data.

    :param client_data: ClientFactsModel instance containing client information
    :type client_data: ClientFactsModel
    :param arguments: Arguments containing portfolio name
    :type arguments: dict[str, Any]
    :returns: Created PortfolioFactsModel instance
    :rtype: PortfolioFactsModel
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

    model = PortfolioFactsFactory.get_model(client)
    portfolio = model(
        client=client,  # Fixed: use lowercase field names
        portfolio=portfolio_name,  # Fixed: use lowercase field names
        contacts=[ContactFacts(name="John Doe", email="john.doe@example.com")],  # Fixed: email domain
        approvers=[
            ApproverFacts(
                name="Jane Doe",
                email="jane.doe@example.com",
                roles=["admin"],
                sequence=1,
            )
        ],  # Fixed: email
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


def get_zone_data(client_data: ClientFactsModel, arguments: dict[str, Any]) -> ZoneFactsModel:
    """
    Create and save ZoneFactsModel test data.

    :param client_data: ClientFactsModel instance containing client information
    :type client_data: ClientFactsModel
    :param arguments: Arguments dictionary (unused but kept for consistency)
    :type arguments: dict[str, Any]
    :returns: Created ZoneFactsModel instance
    :rtype: ZoneFactsModel

    Examples
    --------
    >>> zone_facts = get_zone_data(client_facts, {})
    """
    # Fixed: Use lowercase attribute access
    automation_account_id = client_data.AutomationAccount
    automation_account_name = client_data.OrganizationAccount
    client = client_data.Client  # Fixed: use lowercase field names

    model = ZoneFactsFactory.get_model(client)
    zone = model(
        client=client_data.Client,  # Fixed: use lowercase field names
        zone="my-automation-service-zone",
        account_facts=AccountFacts(  # Fixed: use lowercase field names
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
            "sin": RegionFacts(
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


def get_app_data(portfolio_data: PortfolioFactsModel, zone_data: ZoneFactsModel, arguments: dict[str, Any]) -> AppFactsModel:
    """
    Create and save AppFactsModel test data.

    :param portfolio_data: PortfolioFactsModel instance containing portfolio information
    :type portfolio_data: PortfolioFactsModel
    :param zone_data: ZoneFactsModel instance containing zone information
    :type zone_data: ZoneFactsModel
    :param arguments: Arguments containing app name
    :type arguments: dict[str, Any]
    :returns: Created AppFactsModel instance
    :rtype: AppFactsModel
    :raises AssertionError: If 'app' key is not in arguments

    Examples
    --------
    >>> args = {"app": "api"}
    >>> app_facts = get_app_data(portfolio_facts, zone_facts, args)
    """
    assert "app" in arguments, "App name must be provided in arguments"

    # The client/portfolio is where this BizApp that this Deployment is for.
    # The Zone is where this BizApp component will be deployed.

    client = portfolio_data.Client
    portfolio = portfolio_data.Portfolio
    app = arguments["app"]

    client_portfolio_key = f"{client}:{portfolio}"

    model = AppFactsFactory.get_model(client)
    app_facts = model(
        client_portfolio=client_portfolio_key,
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


def initialize(
    arguments: dict[str, Any],
) -> tuple[ClientFactsModel, ZoneFactsModel, PortfolioFactsModel, AppFactsModel]:
    """
    Initialize all test data for deployspec testing.

    Creates all required test data including client, zone, portfolio, and app facts.

    :param arguments: Arguments containing client, portfolio, and app names
    :type arguments: dict[str, Any]
    :returns: Tuple of created test data instances
    :rtype: tuple[ClientFactsModel, ZoneFactsModel, PortfolioFactsModel, AppFactsModel]
    :raises Exception: If DynamoDB bootstrap fails

    Examples
    --------
    >>> args = {"client": "acme", "portfolio": "core", "app": "api"}
    >>> client_data, zone_data, portfolio_data, app_data = initialize(args)
    """
    if not bootstrap_dynamo():
        raise Exception("Failed to bootstrap DynamoDB")

    org_data = get_organization(False)

    client_data: ClientFactsModel = get_client_data(org_data, arguments)
    zone_data: ZoneFactsModel = get_zone_data(client_data, arguments)
    portfolio_data: PortfolioFactsModel = get_portfolio_data(client_data, arguments)
    app_data: AppFactsModel = get_app_data(portfolio_data, zone_data, arguments)

    return client_data, zone_data, portfolio_data, app_data

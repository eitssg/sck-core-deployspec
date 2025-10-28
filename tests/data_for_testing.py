import os
import core_framework as util

from core_db.registry.client import ClientFact, ClientActions
from core_db.registry.portfolio import PortfolioFact, PortfolioActions
from core_db.registry.app import AppFact, AppActions
from core_db.registry.zone import ZoneFact, ZoneActions


def __load_data(name: str) -> dict:
    dirname = os.path.dirname(os.path.realpath(__file__))
    fn = os.path.join(dirname, name)
    with open(fn, "r") as f:
        return util.clean_yaml(util.read_yaml(f))


def get_client_data() -> ClientFact:
    data = __load_data("facts-client.yaml")
    cf = ClientFact.model_validate(data)
    return ClientActions.create(record=cf)


def get_portfolio_data(client: str) -> PortfolioFact:
    data = __load_data("facts-portfolio.yaml")
    portfolio = PortfolioFact.model_validate(data)
    return PortfolioActions.create(client=client, record=portfolio)


def get_zone_data(client: str) -> ZoneFact:
    data = __load_data("facts-zone.yaml")
    zone = ZoneFact.model_validate(data)
    return ZoneActions.create(client=client, record=zone)


def get_app_data(client: str) -> AppFact:
    data = __load_data("facts-app.yaml")
    app = AppFact.model_validate(data)
    return AppActions.create(client=client, record=app)


def initialize() -> tuple[ClientFact, ZoneFact, PortfolioFact, AppFact]:

    client_data: ClientFact = get_client_data()
    zone_data: ZoneFact = get_zone_data(client=client_data.client)
    portfolio_data: PortfolioFact = get_portfolio_data(client=client_data.client)
    app_data: AppFact = get_app_data(client=client_data.client)

    return client_data, zone_data, portfolio_data, app_data

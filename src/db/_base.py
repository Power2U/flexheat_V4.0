import abc
import models
import typing
import pytz
from mpc.params import PhysicalHouseParameters, HouseSimulationParameters, MPCConfiguration


class HouseSensorRepository:


    def __init__(self, session):
        self.session = session

    @abc.abstractmethod
    def get_data_for_subcentral(self, house: models.House, time_range, now: pytz.datetime.datetime):
        pass

    def get_data_for_building(self, house: models.House, time_range, now: pytz.datetime.datetime):
        pass

class HouseModelRepository:
    def __init__(self, session):
        self.session = session

    @abc.abstractmethod
    def get_model_by_house(self, house: models.House, horizon: int):
        pass

    @abc.abstractmethod
    def get_parameters_by_house(self, house: models.House) -> typing.Tuple[PhysicalHouseParameters, HouseSimulationParameters, MPCConfiguration]:
        pass

    @abc.abstractmethod        
    def get_heatcurve_by_house(self, house: models.House, planning_start):
        pass

class WeatherRepository:
    def __init__(self, session):
        self.session = session

    @abc.abstractmethod
    def get_weather_by_location(self, house: models.House):
        pass
    
class FlexibilityModelRepository:
    def __init__(self, session):
        self.session = session

    @abc.abstractmethod
    def get_config_by_customer(self, customer):
        pass

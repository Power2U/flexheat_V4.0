from dataclasses import dataclass
from dataclasses_json import dataclass_json
import typing
import pytz


@dataclass_json
@dataclass(eq=True, frozen=True)
class House:
    location: str
    customer_id: int
    subcentral_id: int
    longitude: float
    latitude: float
    grid_zone: int

@dataclass_json
@dataclass(eq=True, frozen=True)
class HeatCurve:
    break_point: int
    out_temp: typing.List[float] # Ascending
    inflow_temp: typing.List[float] # Descending
    power: typing.List[float] # Descending

@dataclass_json
@dataclass(eq=True, frozen=True)
class ModelOutput:
    customer_id: int
    subcentral_id: int
    tstamp_record: pytz.datetime.datetime
    ts_start: pytz.datetime.datetime
    ts_end: pytz.datetime.datetime
    out_temp: float
    baseline_power: float
    scheduled_power: float
    power_offset: float
    inflow_temp_offset: float
    indoor_temp_estimate: float
    solar_irradiation: float
    scheduled_inflow_temp: float

@dataclass_json
@dataclass(eq=True, frozen=True)
class Plan:
    customer_id: int
    subcentral_id: int
    grid_zone: int
    tstamp_record: pytz.datetime.datetime
    ts_start: pytz.datetime.datetime
    ts_end: pytz.datetime.datetime
    heating_baseline: float
    outside_temperature: float
    heating_power: float
    power_offset: float
    average_indoor_temperature: float

@dataclass_json
@dataclass(eq=True, frozen=True)
class Dispatch:
    customer_id: int
    subcentral_id: int
    grid_zone: int
    tstamp_record: pytz.datetime.datetime
    ts_start: pytz.datetime.datetime
    ts_end: pytz.datetime.datetime
    power_offset: float

@dataclass_json
@dataclass(eq=True, frozen=True)
class Report:
    customer_id: int
    subcentral_id: int
    grid_zone: int
    tstamp_record: pytz.datetime.datetime
    ts_start: pytz.datetime.datetime
    ts_end: pytz.datetime.datetime
    power_offset: float
    heating_baseline: float
    heating_power: float
    average_indoor_temperature: float
    
@dataclass_json
@dataclass(eq=True, frozen=True)
class OutTempDeviationForecast:
    customer_id: int
    subcentral_id: int
    timestamp: pytz.datetime.datetime
    measure_forecast_deviation: float
    tstamp_record: pytz.datetime.datetime

@dataclass_json
@dataclass(eq=True, frozen=True)
class DynamicIndoorModel:
    customer_id: int
    subcentral_id: int
    tstamp_record: pytz.datetime.datetime
    intercept: float
    variable_coef: typing.List[float]
    

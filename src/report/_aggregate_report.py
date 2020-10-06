from forecasters._forecast_auto_dynamic import HouseDataForecaster
from db._cassandra import CassandraAggregateRepository
from db._models import FlexibilityModelRepository
import pytz
import models
import math
import numpy as np

import logging

logger = logging.getLogger("__main__")

class AggregationReport():
     
    def __init__(self, 
                 customer,
                 grid,
                 aggregate_repo: FlexibilityModelRepository,
                 flexibility_repo: CassandraAggregateRepository,
                 report_end: pytz.datetime.datetime,
                 ):
        
        self._customer = customer
        self._grid = grid
        self._aggregate_repo = aggregate_repo
        self._config = self._aggregate_repo.get_config_by_customer(self._customer)
        self._horizon = self._config.planning_horizon
        self._timestep = self._config.timestep
        self._flexibility_repo = flexibility_repo
        self._report_end = report_end
        self._time_range = self.get_time_range()
          
    # Get time range to define the time scale for planning
    def get_time_range(self):
          
        logger.info("Getting time range for report data:")        
        report_past = (self._horizon) * self._timestep
        time_range = [timedelta(seconds = report_past), timedelta(seconds = 0)]                

        logger.info(f"{time_range}")        
        return time_range           
         
    def aggregate_report(self, subcentral_reports):
        
        index = pd.date_range(self._report_end - self._time_range[0], self._planning_start + self._time_range[1], freq = f"{self._timestep}S")
        data_report = pd.DataFrame(index = index)
        for subcentral in subcentral_reports:
            data_report.join(subcentral.power_offset, how = 'outer')
        data_report['aggregate_power_offset'] = data_report.sum(axis = 1)
        
        return data_report 
        

from db._cassandra import CassandraAggregateRepository
from db._models import FlexibilityModelRepository
import pytz
import models
import math
import numpy as np
import pandas as pd
from datetime import timedelta
import logging

logger = logging.getLogger("__main__")

class AggregationReport():
     
    def __init__(self, 
                 customer,
                 grid,
                 aggregate_repo: FlexibilityModelRepository,
                 flexibility_repo: CassandraAggregateRepository,
                 report_start: pytz.datetime.datetime,
                 ):
        
        self._customer = customer
        self._grid = grid
        self._aggregate_repo = aggregate_repo
        self._config = self._aggregate_repo.get_config_by_customer(self._customer)
        self._horizon = self._config.planning_horizon
        self._timestep = self._config.timestep
        self._flexibility_repo = flexibility_repo
        self._report_start = report_start
        self._time_range = self.get_time_range()
          
    # Get time range to define the time scale for planning
    def get_time_range(self):
          
        logger.info("Getting time range for report data:")        
        report_len = (self._horizon - 1) * self._timestep
        time_range = [timedelta(seconds = 0), timedelta(seconds = report_len)]                

        logger.info(f"{time_range}")        
        return time_range        
         
    def aggregate_report(self, subcentral_reports):
        
        index = pd.date_range(self._report_start - self._time_range[0], self._report_start + self._time_range[1], freq = f"{self._timestep}S")
        data_report = pd.DataFrame(index = index)
        
        if len(subcentral_reports) == 0:
            
            logger.warning("Subcentral reports are not available!")
            data_report['aggregate_power_offset'] = 0
        
        else:
            for subcentral in subcentral_reports:
                data_report = pd.concat([data_report, subcentral.power_offset.resample(timedelta(seconds = self._timestep)).mean()], axis = 1, join = 'outer')
            data_report['aggregate_power_offset'] = data_report.sum(axis = 1)
        
        return data_report 
        
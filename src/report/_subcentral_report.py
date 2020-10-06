import pandas as pd
from db._cassandra import CassandraRepository
from db._models import HouseModelRepository
import pytz
import models
from mpc.params import FlexibilityConfiguration
import math
from utils import *
import numpy as np
from datetime import timedelta
import logging

logger = logging.getLogger("__main__")

class SubcentralReport():
    
    def __init__(self, house: models.House, 
                 cassandra_repo: CassandraRepository, 
                 house_repo: HouseModelRepository, 
                 report_end: pytz.datetime.datetime,
                 utility_config: FlexibilityConfiguration):
        
        self._house = house
        self._cassandra_repo = cassandra_repo
        phys, _, config, _, dynamic = house_repo.get_parameters_by_house(self._house)
        self._heatcurve = house_repo.get_heatcurve_by_house(self._house, self._his_end)
        self._report_horizon = utility_config.planning_horizon
        self._report_timestep = utility_config.timestep
        self._report_end = report_end
        self._time_range = self.get_time_range()                       
        self.report = self.source_data()
        
        
    def get_time_range(self):
        
        logger.info("Getting time range for report data:")        
        report_past = (self._report_horizon) * self._report_timestep # Generate index for the start of each time step e.g. 0-47        
        time_range = [timedelta(seconds = report_past), timedelta(seconds = 0)]                

        logger.info(f"{time_range}")        
        return time_range
     
    def source_data(self):
        
        logger.info("Preparing for source data")
        index = pd.date_range(self._report_end - self._time_range[0], self._his_end + self._time_range[1], freq = f"{self._report_timestep}S")
        his = pd.DataFrame(index = index)
        
        self._all_data = self._cassandra_repo.get_all_by_house_resample(
            house = self._house,
            time_range=self._time_range,
            now=self._report_past,
            timestep = self._report_timestep
        ).join(his, how = 'outer')                
        
        if not ('measured_outside_temp' in self._all_data.columns):
            logger.warning(f"measured_outside_temp is not available")
            self._all_data['measured_outside_temp'] = np.nan  

        if not ('measure_forecast_deviation' in self._all_data.columns):
            logger.warning(f"measure_forecast_deviation is not available")
            self._all_data['measure_forecast_deviation'] = 0 
                                     
        for index, row in self._all_data.iterrows():
            if np.isnan(row['measured_outside_temp']):                
                self._all_data.at[index, 'measured_outside_temp'] = row['forecast_outside_temp'] + row['measure_forecast_deviation']  
            
            # default value of average_indoor_temperature was set as -99 before Aug 2020             
            if row['average_indoor_temperature'] == -99:
                self._all_data.at[index, 'average_indoor_temperature'] = np.nan
                           
        self._all_data.interpolate(method='linear', limit_direction='forward', inplace = True)
        self._all_data.interpolate(method='linear', limit_direction='backward', inplace = True) 
                                                         
        self.heat_power()
        self.baselin_power()
        self._all_data['power_offset'] = self._all_data['heat_power'] - self._all_data['baseline_power']
        
        return self._all_data
                                                                                                                                                                        
    # Estimate baseline power according to outside temperature
    def baseline_power(self):
        
        logger.info("Calculating baseline power according to heat curve and measured_outside_temp")
                        
        for index, row in self._all_data.iterrows():
            
            baseline_power = estimate_baseline_power(self.heatcurve, row['measured_outside_temp'])
            self._all_data.at[index, 'baseline_power'] = baseline_power

    # Estimate heat power according to inflow_temp
    def heat_power(self):

        logger.info("Calculating heat power according to heat curve and inflow_temp")
       
        for index, row in self._all_data.iterrows():
            
            heat_power = estimate_initial_power(self.heatcurve, row['inflow_temp'])
            self._all_data.at[index, 'heat_power'] = heat_power     
                                      
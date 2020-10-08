from forecasters._base import HouseForecaster
from db import CassandraRepository
from db._models import HouseModelRepository
import pytz
import models
import math
import numpy as np
import pandas as pd
from utils import *
from datetime import timedelta
import logging
from cvxpy.atoms.affine import index
from db import *
from pandas.core.frame import DataFrame

logger = logging.getLogger("__main__")

class HouseDataForecaster(HouseForecaster):
    
    __FORECAST_TOLERANCE = 1/3
    
    def __init__(self, house: models.House, 
                 cassandra_repo: CassandraRepository, 
                 house_repo: HouseModelRepository, 
                 planning_start: pytz.datetime.datetime,
                 grid_peak: DataFrame):
        
        self._house = house
        self._cassandra_repo = cassandra_repo        
        self._house_repo = house_repo
        self._planning_start = planning_start  
        self._grid_peak = grid_peak
        _, _, config, _, dynamic = self._house_repo.get_parameters_by_house(self._house)
        self._horizon = config.optimization_horizon
        self._timestep = config.timestep
              
        # Add for auto dynamic model
        self._in_temp_lag = dynamic.in_temp_diff_lag
        self._out_temp_lag = dynamic.out_temp_diff_lag
        self._solar_lag = dynamic.solar_diff_lag
        
        self._time_range, self._initial_range = self.get_time_range()
        self.heatcurve = self._house_repo.get_heatcurve_by_house(self._house, self._planning_start)
        self.source_data()
        
    # Get time range to define the time scale for forecasting and planning
    def get_time_range(self):
        
        logger.info("Getting time range for training data:")        
        
        # Add for auto dynamic model        
        max_lag = max(max(self._in_temp_lag), max(self._out_temp_lag), max(self._solar_lag))        
        past_lag = self._timestep * (max_lag + 1)        
        range_future = (self._horizon - 1) * self._timestep # Generate index for the start of each time step e.g. 0-47        
        time_range = [timedelta(seconds = past_lag), timedelta(seconds = range_future)]
        
        past_initial = self._timestep
        initial_range = [timedelta(seconds = past_initial), timedelta(seconds = 0)]

        logger.info(f"time_range = {time_range}")
        logger.info(f"initial_range = {initial_range}")
 
        return time_range, initial_range

    # Prepare inputs for MPC planning
    def source_data(self):
        
        logger.info("Preparing for source data")                
        index = pd.date_range(self._planning_start - self._time_range[0], self._planning_start + self._time_range[1], freq = f"{self._timestep}S")
        data_planning = pd.DataFrame(index = index)
        self._all_data = self._cassandra_repo.get_all_by_house_resample(
            house = self._house,
            time_range = self._time_range,
            now = self._planning_start,
            timestep = self._timestep
        ).join(data_planning, how = 'outer')
                       
        # Predict outside temperature: forecast_outside_temp + measure_forecast_deviation
        # Pilot tests show that the measured outside temperature can deviate from weather forecast due to e.g. direct sunshine
        # measure_forecast_deviation is predicted from a trained SARIMAX model        
        if not ('measured_outside_temp' in self._all_data.columns):
            logger.warning(f"measured_outside_temp is not available")
            self._all_data['measured_outside_temp'] = np.nan  

        if not ('measure_forecast_deviation' in self._all_data.columns):
            logger.warning(f"measure_forecast_deviation is not available")
            self._all_data['measure_forecast_deviation'] = 0 
            
        self._all_data['out_temp'] = np.nan  
        
        for index, row in self._all_data.iterrows():
            
                if not np.isnan(row['measured_outside_temp']):
                    self._all_data.at[index, 'out_temp'] = self._all_data.loc[index, 'measured_outside_temp'] 
                
                else:                    
                    if np.isnan(row['measure_forecast_deviation']):
                        self._all_data.at[index, 'measure_forecast_deviation'] = 0                                                         
                    self._all_data.at[index, 'out_temp'] = self._all_data.loc[index, 'forecast_outside_temp'] \
                                                           + self._all_data.loc[index, 'measure_forecast_deviation']

                # default value of average_indoor_temperature was set as -99 before Aug 2020             
                if row['average_indoor_temperature'] == -99:
                    self._all_data.at[index, 'average_indoor_temperature'] = np.nan
                                                                               
        self._all_data.interpolate(method='linear', limit_direction='forward', inplace = True)
        self._all_data.interpolate(method='linear', limit_direction='backward', inplace = True)  
                            
        self.baseline_power()                                               
        self.solar()        
        self.cal_diff()              
        self.forecast_data = self._all_data.loc[self._planning_start:] # Forecast inputs for the planning horizon
        # Add for auto dynamic model
        self.diff_data = self._all_data[['solar_diff', 'out_temp_diff', 'in_temp_diff']].iloc[1:]
        
        # Initial condition for the planning horizon
        initial_data = self._cassandra_repo.get_initial_by_house(
            house = self._house,
            time_range = self._initial_range,
            now = self._planning_start)
        
        initial_data.interpolate(method='linear', limit_direction='forward', inplace = True)
        initial_data.interpolate(method='linear', limit_direction='backward', inplace = True)          
        self.initial_data = initial_data.iloc[-1] # The last item before self._planning_start is considered as the initial point        
        self.initial_status()
        
        # Add for flexibility service
        self.peak_hour()
        self.dispatch()
        
    # Estimate baseline power according to heat curve and predicted outside temperature
    def baseline_power(self):
        
        logger.info("Calculating baseline power according to heat curve and predicted outside temperature")                
        for index, row in self._all_data.iterrows():            
            baseline_power = estimate_baseline_power(self.heatcurve, row['out_temp'])
            self._all_data.at[index, 'baseline_power'] = baseline_power
                    
    # Estimate solar irradiation according to forecast cloud cover
    def solar(self):
        
        logger.info("Calculating solar irradiation")                    
        predict_solar = estimate_solar(
            house = self._house,
            cloud_cover = self._all_data.forecast_cloud_cover,
        )
        
        self._all_data['predict_solar'] = predict_solar / 1000  # kW/m^2   
        
    # Estimate initial heating power before the MPC planning
    def initial_status(self):

        logger.info("Calculating initial heat power according to inflow_temp")                                    
        self.initial_data['heat_power'] = estimate_initial_power(
            heatcurve = self.heatcurve,
            inflow_temp = self.initial_data['inflow_temp']  
            )
 
    # Add for auto dynamic model, 1st order differencing                       
    def cal_diff(self):

        logger.info("Calculating 1st order difference")                            
        self._all_data['solar_diff'] = self._all_data['predict_solar'].diff()
        self._all_data['out_temp_diff'] = self._all_data['out_temp'].diff()
        
        for index, row in self._all_data.iterrows():           
            if index < self._planning_start:
                self._all_data['in_temp_diff'] = self._all_data['average_indoor_temperature'].diff()
                
    # Generate optimization model for the subcentral
    # Optimization model for flexibility service plan
    def optimization_model_plan(self):
        
        # Add for auto dynamic model
        return self._house_repo.get_model_by_house_plan(self._house)

    # Add for flexibility service
    # Optimization model for flexibility service execution
    def optimization_model_execution(self):
        
        # Add for auto dynamic model
        return self._house_repo.get_model_by_house_execution(self._house)
        
    # Add for flexibility service
    # Set peak hours for planning horizon
    def peak_hour(self):
        
        self.forecast_data['peak_hour'] = 0      
         
        if not self._grid_peak.empty:
            
            for index_forecast, row_forecast in self.forecast_data.iterrows():
                for index_peak, row_peak in self._grid_peak.iterrows():
                    if index_forecast >= row_peak['ts_start'] and index_forecast < row_peak['ts_end']:
                        self.forecast_data.at[index_forecast, 'peak_hour'] = 1              
         
#     peak = self._flexibility_repo.get_peak_by_customer(
#         customer = self._customer,
#         grid = self._grid,
#         time_range = self._time_range,
#         now = self._planning_start
#     )         
#         if not peak.empty:                           
#             for index_all, row_all in self._all_data.iterrows():
#                 for index_peak, row_peak in peak.iterrows():
#                     if index_all >= row_peak['ts_start'] and index_all <= row_peak['ts_end']:
#                         self._all_data.at[index_all, 'peak_hour'] = 1  
    
    # Add for flexibility service
    # Set dispatch order for planning horizon                  
    def dispatch(self):
        
#         self._all_data['dispatch_increase'] = 0         
#         self._all_data['dispatch_decrease'] = 0 
        
        dispatch = self._cassandra_repo.get_dispatch_by_house(
            house = self._house,
            time_range = self._time_range,
            now = self._planning_start
        )
        
        if not dispatch.empty:
            dispatch.fillna(0)
            self.forecast_data = self.forecast_data.join(dispatch, how = 'outer')
                 
            for index, row in self.forecast_data.iterrows():
                if row['peak_hour'] == 1:
                    if row['subcentral_dispatch'] > 0:
                        logger.error(f"Dispatch error: Power increase is dispatched for peak hour: {index}")
        else:
            self.forecast_data['subcentral_dispatch'] = 0      

#                 if row['subcentral_dispatch'] >= 0:
#                     self._all_data.at[index, 'dispatch_increase'] = row_dispatch['subcentral_dispatch']         
#                     self._all_data.at[index, 'dispatch_decrease'] = 0
#                 else:
#                     self._all_data.at[index, 'dispatch_increase'] = 0
#                     self._all_data.at[index, 'dispatch_decrease'] = - row_dispatch['subcentral_dispatch']                               
      
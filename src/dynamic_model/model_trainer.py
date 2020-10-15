import pandas as pd
from db._cassandra import CassandraHouseRepository, CassandraWeatherRepository, CassandraRepository
from db._models import HouseModelRepository
import pytz
from sklearn.linear_model import (LinearRegression,
                                  HuberRegressor)
import models
import math
from utils import *
import numpy as np
from datetime import timedelta

from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from sklearn.metrics import mean_squared_error
import logging

logger = logging.getLogger("__main__")

class DynamicModelGenerator():
    
    __FORECAST_TOLERANCE = 1/3
    
    def __init__(self, house: models.House, 
                 cassandra_house_repo: CassandraHouseRepository,
                 cassandra_weather_repo: CassandraWeatherRepository,
                 cassandra_repo :CassandraRepository,
                 house_model_repo: HouseModelRepository,              
                 his_end: pytz.datetime.datetime):
        
        self._house = house
        self._cassandra_house_repo = cassandra_house_repo
        self._cassandra_weather_repo = cassandra_weather_repo
        self._cassandra_repo = cassandra_repo
        
        self._his_end = his_end       
        phys, _, config, _, dynamic = house_model_repo.get_parameters_by_house(self._house)
        
        self._horizon = config.optimization_horizon
        self._timestep = config.timestep
        
        self._train_len = dynamic.training_length        
        self._in_temp_lag = dynamic.in_temp_diff_lag
        self._out_temp_lag = dynamic.out_temp_diff_lag
        self._solar_lag = dynamic.solar_diff_lag
        self._max_lag = max(max(self._in_temp_lag), max(self._out_temp_lag), max(self._solar_lag))          
        
        self._heat_loss_coeff = phys.heat_loss_coeff
        self._capacitance = phys.capacitance
        
        self._time_range = self.get_time_range()        
        self.heatcurve = house_model_repo.get_heatcurve_by_house(self._house, self._his_end)
               
        self.source_data()
        self._train_x, self._train_y = self.training_data()
        self.model_coef, self.model_intercept, self._model = self.train_model()
        self.mse = self.test_model()
        
    def get_time_range(self):
        
        logger.info("Getting time range for training data:")        
        past_train = self._timestep * (self._train_len + self._max_lag + 2) # additional two times steps for differencing and get initial/after value
        
        time_range = [timedelta(seconds = past_train), timedelta(seconds = 0)]
        
        logger.info(f"{time_range}")
        
        return time_range
     
    def source_data(self):
        
        logger.info("Preparing for source data")
        index = pd.date_range(self._his_end - self._time_range[0], self._his_end + self._time_range[1], freq = f"{self._timestep}S")
        his = pd.DataFrame(index = index)
        
# =======================================================================================================
        
# Version a: use previous sent schedules to estimate heat power
        
#         # Get measured_outside_temp and inflow_temp, resample by timestep
#         subcentral_data = self._cassandra_house_repo.get_data_for_subcentral(
#             house = self._house,
#             time_range = self._time_range,
#             now = self._his_end
#         ).resample(timedelta(seconds = self._timestep)).mean()        
# 
#         # Get average_indoor_temperature, resample by timestep
#         building_data = self._cassandra_house_repo.get_data_for_building(
#             house = self._house,
#             time_range = self._time_range,
#             now = self._his_end
#         ).resample(timedelta(seconds = self._timestep)).first()     
#            
#         # Get scheduled_power
#         schedule_data = self._cassandra_house_repo.get_schedules_by_house(
#             self._house,
#             time_range = self._time_range,
#             now = self._his_end)
#          
#         if not schedule_data.empty:            
#             schedule_data = schedule_data.resample(timedelta(seconds = self._timestep)).mean()   
#         
#         # Get forecast cloud cover and outside temperature
#         weather_forecast = self._cassandra_weather_repo.get_weather_by_location(
#             house = self._house,
#             time_range = self._time_range,
#             now = self._his_end
#         ).resample(timedelta(seconds = self._timestep)).mean()  
#         
#         self._all_data = his.join(subcentral_data, how = 'outer').\
#                              join(building_data, how = 'outer').\
#                              join(schedule_data, how = 'outer').\
#                              join(weather_forecast, how = 'outer')        
        
#         self.baseline_power()   
#         self._all_data['heat_power'] = np.nan
#         
#         for index, row in self._all_data.iterrows():
#             if np.isnan(row['scheduled_power']):
#                 self._all_data.at[index, 'heat_power'] = row['baseline_power']
#             else:
#                 self._all_data.at[index, 'heat_power'] = row['scheduled_power']   
 
# =======================================================================================================

# Version b: use inflow temperature to estimate heat power, in case the schedules have not been sent or the actual inflow temperature deviate from schedules        
        
        self._all_data = self._cassandra_repo.get_all_by_house_resample(
            house = self._house,
            time_range=self._time_range,
            now=self._his_end,
            timestep = self._timestep
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
                                                         
        self.solar()  
        self.heat_power()                                                                   
        self.cal_diff()                   
                                               
    def training_data(self):

        logger.info("Preparing for training set")
        
        train_start = self._max_lag + 1 # Skip the 1st row of self._all_data since the diff is NaN
        train_end = -2
        
        temperature_initial =  self._all_data['average_indoor_temperature'].iloc[train_start: train_end].values.reshape(-1, 1)
        solar = self._all_data['predict_solar'].iloc[train_start: train_end].values.reshape(-1, 1)
        power = self._all_data['heat_power'].iloc[train_start: train_end].values.reshape(-1, 1)
#         in_out_difference = ((self._all_data['average_indoor_temperature'].iloc[train_start: train_end].values \
#                             - self._all_data['measured_outside_temp'].iloc[train_start: train_end].values) \
#                               * self._heat_loss_coeff).reshape(-1, 1)   
        in_out_difference = (self._all_data['average_indoor_temperature'].iloc[train_start: train_end].values \
                            - self._all_data['measured_outside_temp'].iloc[train_start: train_end].values).reshape(-1, 1)                                          
        in_temp_diff = []
        out_temp_diff = []
        solar_diff = []
        
        for i in self._in_temp_lag:
            in_temp_diff.append(self._all_data['in_temp_diff'].iloc[train_start - i: train_end - i].values)
        
        for i in self._out_temp_lag:
            out_temp_diff.append(self._all_data['out_temp_diff'].iloc[train_start - i: train_end - i].values)
                                  
        for i in self._solar_lag:
            solar_diff.append(self._all_data['solar_diff'].iloc[train_start - i: train_end - i].values)
            
        temperature_target = self._all_data['average_indoor_temperature'].iloc[train_start + 1: train_end + 1].values

        # Define model variables        
        '''             
        Sequence matters!!!
        Sequence keeps consistent with ../mpc/greybox_auto_dynamic.py: PnPkModel_AUTO_DYNAMIC.dynamics()  
                                        
        Alternative: 
        The structure of x can be adjusted flexibly e.g. delete the last three diff components, 
        then the model structure will be similar as in flexheat V2.0
     
        '''                    
        x = np.concatenate((temperature_initial,
                                      solar,
                                      power,
                                      in_out_difference,
                                      np.array(in_temp_diff).T,
                                      np.array(out_temp_diff).T,
                                      np.array(solar_diff).T), axis = 1)
        
# Alternative: Multiply indoor-outdoor temperature difference with house.heat_loss_coeff, reflecting physical relationships
#        train_x = x * self._timestep / self._capacitance
        train_x = x
                                      
        train_y = temperature_target
        
        return train_x, train_y
                        
    def train_model(self):
    
        logger.info("Start training dynamic model")
    
        estimator = LinearRegression(fit_intercept = True)

        model = make_pipeline(PolynomialFeatures(1), estimator)

        model.fit(self._train_x, self._train_y)
        
        model_coef = model[1].coef_
        model_intercept = model[1].intercept_
                
        logger.info(f"model_coef = {model_coef}")
        logger.info(f"model_intercept = {model_intercept}")

        return model_coef, model_intercept, model
        
    # Calculate in-sample prediction error
    def test_model(self):
        
        logger.info("Calculating in-sample error")
    
        predict_y = self._model.predict(self._train_x)
        mse = mean_squared_error(predict_y, self._train_y)
        
        logger.info(f"insample mse = {mse}")
        
        return mse

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
                                      
    # Apply first differencing
    def cal_diff(self):
        
        logger.info("Calculating 1st order difference")

        self._all_data['solar_diff'] = self._all_data['predict_solar'].diff()                   
        self._all_data['out_temp_diff'] = self._all_data['measured_outside_temp'].diff()
        self._all_data['in_temp_diff'] = self._all_data['average_indoor_temperature'].diff()

    # Estimate solar irradiation according to forecast cloud cover
    def solar(self):
        
        logger.info("Calculating solar irradiation")
    
        predict_solar = estimate_solar(
            house = self._house,
            cloud_cover = self._all_data.forecast_cloud_cover,
        )
        
        self._all_data['predict_solar'] = predict_solar / 1000  # unit: kW/m^2   

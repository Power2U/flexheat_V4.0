import pandas as pd
from db._cassandra import CassandraHouseRepository, CassandraWeatherRepository
from db._models import HouseModelRepository
import pytz
import models
import math
import numpy as np
from datetime import timedelta
from statsmodels.tsa.statespace.sarimax import SARIMAXResults
import pickle
import logging

logger = logging.getLogger("__main__")

class OutTempPredictor():
    
    __FORECAST_TOLERANCE = 1/3
    
    def __init__(self, house: models.House, \
                 cassandra_house_repo: CassandraHouseRepository, \
                 cassandra_weather_repo: CassandraWeatherRepository, \
                 house_model_repo: HouseModelRepository, \
                 planning_start: pytz.datetime.datetime,\
                 model):
        
        self._house = house
        self._cassandra_house_repo = cassandra_house_repo
        self._cassandra_weather_repo = cassandra_weather_repo
        self._planning_start = planning_start       
        _, _, mpc_config, sarimax_config, _ = house_model_repo.get_parameters_by_house(self._house)
        
        self._horizon = mpc_config.optimization_horizon
        self._timestep = mpc_config.timestep
        
        sarimax_param = sarimax_config.sarimax_pdq_PDQ_s
        if len(sarimax_param) != 7:                        
            raise ValueError(f"SARIMAX model configuration is wrong for cid = {self._house.customer_id}, sid = {self._house.subcentral_id}")
        else:
            self._p = sarimax_param[0]
            self._d = sarimax_param[1]
            self._q = sarimax_param[2]
            self._P = sarimax_param[3]
            self._D = sarimax_param[4]
            self._Q = sarimax_param[5]
            self._s = sarimax_param[6]
            
        self._his_range = self.get_his_range()        
        self._his_data = self.his_data()
        self._pred_range = self.get_pred_range()  
        
        with open(model, 'rb') as f:
            self._model = pickle.load(f)          
            
        self.forecast = self.prediction()    
                
    def get_his_range(self):

        logger.info("Getting time range for historical data:")                
        arima_lag = max(self._p, self._q)
        sarima_lag = max(self._P * self._s, self._Q * self._s)
        lag = max(arima_lag, sarima_lag)
        his = self._timestep * lag
        
        time_range = [timedelta(seconds = his), timedelta(seconds = 0)]
        logger.info(f"{time_range}")
         
        return time_range
    
    def get_pred_range(self):
        
        logger.info("Getting time range for prediction:")                
        pred = self._timestep * (self._horizon - 1)
        
        pred_range = [timedelta(seconds = 0), timedelta(seconds = pred)]
        logger.info(f"{pred_range}")
         
        return pred_range
     
    # Prepare historical data as the inputs of prediction
    # Lag data is required for the prediction depends on SARIMAX configuration
    def his_data(self):
        
        logger.info("Preparing for historical data")
        index = pd.date_range(self._planning_start - self._his_range[0], self._planning_start + self._his_range[1], freq = f"{self._timestep}S")
        his = pd.DataFrame(index = index)

        subcentral_data = self._cassandra_house_repo.get_data_for_subcentral(
            house = self._house,
            time_range = self._his_range,
            now = self._planning_start
        ).resample(timedelta(seconds = self._timestep)).mean()
        
        forecast_data = self._cassandra_weather_repo.get_weather_by_location(
            house = self._house,
            time_range = self._his_range,
            now = self._planning_start)
        
        his_data = his.join(subcentral_data, how = 'outer').join(forecast_data, how = 'outer')
                 
        if not ('measured_outside_temp' in his_data.columns):
            logger.warning(f"measured_outside_temp in his_range is not available")
            his_data['measured_outside_temp'] = np.nan  

        for index, row in his_data.iterrows():           
                if np.isnan(row['measured_outside_temp']):
                    his_data.at[index, 'measured_outside_temp'] = row['forecast_outside_temp']  
                    
        his_data.interpolate(method='linear', limit_direction='forward', inplace = True)
        his_data.interpolate(method='linear', limit_direction='backward', inplace = True)  
                 
        his_data['forecast_devaition'] = his_data['measured_outside_temp'] - his_data['forecast_outside_temp'] 
        
        his_data = his_data.iloc[:-1]
        
        return his_data
        
    # Predict forecast/measurement deviation for the planning horizon                
    def prediction(self):

        logger.info("Start prediction")        
        endog = self._his_data['forecast_devaition'].values
        model = self._model.apply(endog)
        pred = model.forecast(self._horizon)
        
        index = pd.date_range(self._planning_start + self._pred_range[0], self._planning_start + self._pred_range[1], freq = f"{self._timestep}S")
        forecast = pd.DataFrame(data = pred, index = index, columns = ['forecast_devaition'])
        
        return forecast
        
          
        
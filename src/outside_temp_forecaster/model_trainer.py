import pandas as pd
from db._cassandra import CassandraHouseRepository, CassandraWeatherRepository
from db._models import HouseModelRepository
import pytz
import models
import math
from utils import *
import numpy as np
from datetime import timedelta
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.statespace.sarimax import SARIMAXResults
import logging

logger = logging.getLogger("__main__")

class OutTempModelGenerator():
    
    __FORECAST_TOLERANCE = 1/3
    
    def __init__(self, house: models.House, \
                 cassandra_house_repo: CassandraHouseRepository, \
                 cassandra_weather_repo: CassandraWeatherRepository, \
                 house_model_repo: HouseModelRepository, \
                 his_end: pytz.datetime.datetime):
        
        self._house = house
        self._cassandra_house_repo = cassandra_house_repo
        self._cassandra_weather_repo = cassandra_weather_repo
        self._his_end = his_end       
        _, _, mpc_config, sarimax_config, _ = house_model_repo.get_parameters_by_house(self._house)
        
        self._horizon = mpc_config.optimization_horizon
        self._timestep = mpc_config.timestep
        self._train_len = sarimax_config.sarimax_train_length
        self._sarimax_param = sarimax_config.sarimax_pdq_PDQ_s

        self._time_range = self.get_time_range()        
        self._train_data = self.train_data()
        
        self.model = self.train_model()
        
    def get_time_range(self):

        logger.info("Getting time range for training data:")         
                       
        train_his = self._timestep * self._train_len
        
        time_range = [timedelta(seconds = train_his), timedelta(seconds = 0)]
        logger.info(f"{time_range}")
         
        return time_range
     
    # Prepare historical data to train the model
    def train_data(self):

        logger.info("Preparing for training data")                
        
        index = pd.date_range(self._his_end - self._time_range[0], self._his_end + self._time_range[1], freq = f"{self._timestep}S")
        his = pd.DataFrame(index = index)

        subcentral_data = self._cassandra_house_repo.get_data_for_subcentral(
            house = self._house,
            time_range = self._time_range,
            now = self._his_end
        ).resample(timedelta(seconds = self._timestep)).mean()
                
        forecast_data = self._cassandra_weather_repo.get_weather_by_location(
            house = self._house,
            time_range = self._time_range,
            now = self._his_end)
        
        train_data = his.join(subcentral_data, how = 'outer').join(forecast_data, how = 'outer')

        if not ('measured_outside_temp' in train_data.columns):
            logger.warning(f"measured_outside_temp is not found for cid = {self._house.customer_id}, sid = {self._house.subcentral_id}")
            raise ValueError(f"No data support for fitting new SARIMAX model, old model is kept!")
        
        else:
            for index, row in train_data.iterrows():           
                if np.isnan(row['measured_outside_temp']):
                    train_data.at[index, 'measured_outside_temp'] = row['forecast_outside_temp']  
                                                                               
        train_data.interpolate(method='linear', limit_direction='forward', inplace = True)
        train_data.interpolate(method='linear', limit_direction='backward', inplace = True)          
        train_data['forecast_devaition'] = train_data['measured_outside_temp'] - train_data['forecast_outside_temp']   
        train_data = train_data.iloc[:-1]
        
        return train_data
               
    # Train the SARIMAX model
    def train_model(self):

        logger.info(f"Train SARIMAX model for cid = {self._house.customer_id}, sid = {self._house.subcentral_id}:")        
    
        endog = self._train_data['forecast_devaition'].values            
        
        if len(self._sarimax_param) != 7:                        
            raise ValueError(f"SARIMAX configuration is wrong for cid = {self._house.customer_id}, sid = {self._house.subcentral_id}")
        else:
            p = self._sarimax_param[0]
            d = self._sarimax_param[1]
            q = self._sarimax_param[2]
            P = self._sarimax_param[3]
            D = self._sarimax_param[4]
            Q = self._sarimax_param[5]
            s = self._sarimax_param[6]
            
        sarimax = SARIMAX(endog=endog, order=(p,d,q), seasonal_order=(P,D,Q,s))
        logger.info(f"Start training")        
        model = sarimax.fit()
        logger.debug(f"{model.summary()}")
        
        return model

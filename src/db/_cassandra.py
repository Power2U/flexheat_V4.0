from cassandra.cluster import Cluster
import models
import pytz
from db._base import HouseSensorRepository, WeatherRepository, HouseModelRepository, FlexibilityModelRepository
import typing
import pandas as pd
import logging
import dateutil.parser
from datetime import datetime
from datetime import timedelta
import json
from pandas.core.frame import DataFrame
from mpc.params import FlexibilityConfiguration

logger = logging.getLogger("__main__")

class DBConnection:
       
    def __init__(self, db):
                       
        self.db = db
        self.cluster = Cluster([db])
        self.session = self.cluster.connect() 
        logger.info(f"Connected to database {self.db}")   

    def db_shutdown(self):
        
        self.session.shutdown()
        logger.info(f"Close connection to database {self.db}")   

# Read/write data for subcentral/building                              
class CassandraHouseRepository(HouseSensorRepository):

    _SUBCENTRAL_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.timeseries
         WHERE customer_id = {customer_id}
            AND subcentral_id = {subcentral_id}
            AND asset_id= 6000
            AND tstamp_record >= '{mintime}'
            AND tstamp_record <= '{maxtime}'
            ALLOW FILTERING
    '''    
    
    _APARTMENT_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.average_measurement
         WHERE customer_id = {customer_id}
            AND subcentral_id = {subcentral_id}
            AND tstamp_record >= '{mintime}'
            AND tstamp_record <= '{maxtime}'
            ALLOW FILTERING
    '''   
    
    _FLEXIBILITY_PLAN_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.subcentral_flexibility_plan
         WHERE customer_id = {customer_id}
            AND subcentral_id = {subcentral_id}
            AND ts_start >= '{mintime}'
            AND ts_start <= '{maxtime}'
            ALLOW FILTERING
    '''      
    
    _FLEXIBILITY_DISPATCH_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.subcentral_flexibility_dispatch
         WHERE customer_id = {customer_id}
            AND subcentral_id = {subcentral_id}
            AND ts_start >= '{mintime}'
            AND ts_start <= '{maxtime}'
            ALLOW FILTERING
    '''  
        
    _WRITE_QUERY_TEMPLATE = '''
        INSERT INTO flexheat.mpc_planning JSON 
        '{model_output_json}'
    '''       
    _WRITE_MODEL_QUERY_TEMPLATE = '''
        INSERT INTO flexheat.dynamic_indoor_temperature_model JSON 
        '{dynamic_model_json}'
    '''   
    _WRITE_PLAN_QUERY_TEMPLATE = '''
        INSERT INTO flexheat.subcentral_flexibility_plan JSON 
        '{plan_json}'
    '''    
    _WRITE_DISPATCH_QUERY_TEMPLATE = '''
        INSERT INTO flexheat.subcentral_flexibility_dispatch JSON 
        '{dispatch_json}'
    ''' 
    
    _WRITE_REPORT_QUERY_TEMPLATE = '''
        INSERT INTO flexheat.subcentral_flexibility_report JSON 
        '{report_json}'
    '''        
           
    __TIMEZONE = pytz.timezone('UTC')
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:00Z'
    __TIME_TOLERANCE = 15 * 60 # 15 minutes

    def __init__(self, session):
        
        self.session = session

    def get_data_for_subcentral(self, house: models.House, time_range, now: pytz.datetime.datetime):
        
        now = now
        mintime = now - time_range[0]
        maxtime = now + time_range[1]
        
        logger.info(f"Fetching subcentral data for cid = {house.customer_id}, sid = {house.subcentral_id} in time_range")

        query = self._SUBCENTRAL_QUERY_TEMPLATE.format(customer_id = house.customer_id, subcentral_id = house.subcentral_id, \
                                                       mintime=mintime.strftime(self.__DATETIME_FORMAT), \
                                                       maxtime=maxtime.strftime(self.__DATETIME_FORMAT))
        

        dictionary ={"tstamp":[],"inflow_temp":[], "measured_outside_temp":[], "return_temp":[]}
        print(query)

        rows = self.session.execute(query)
               
        if len(rows._current_rows) == 0:
                    
            raise ValueError(f"No initial subcentral data is found for cid = {house.customer_id}, sid = {house.subcentral_id}, \
                            please check the data communication before starting MPC planning!")
               
        for row in rows:
            
            subcentral_data = json.loads(row.json)
            
            dictionary["tstamp"].append(subcentral_data["tstamp_record"])
            
            heating = subcentral_data["heating_system"]   
            dictionary["inflow_temp"].append(heating["heating_inflow_temperature"]) 
            dictionary["measured_outside_temp"].append(heating["outside_temperature"]) 
            dictionary["return_temp"].append(heating["heating_outflow_temperature"]) 
            
        data = pd.DataFrame(dictionary)
        data.set_index("tstamp", inplace = True) 
        data.index = pd.to_datetime(data.index)
        data.sort_index(ascending = True, inplace = True)
        
#        data = data[data.index <= now]
        
        logger.warning(f"{data.inflow_temp.isnull().sum()} null inflow_temp out of {len(data)}")
        logger.warning(f"{data.measured_outside_temp.isnull().sum()} null measured_outside_temp out of {len(data)}")
        logger.warning(f"{data.return_temp.isnull().sum()} null return_temp out of {len(data)}")
        
        valid_indexes = data.inflow_temp.notnull().index[-1]
        valid_indexes = datetime.strptime(str(valid_indexes), "%Y-%m-%d %H:%M:%S.%f%z")
        
        if (now - valid_indexes).total_seconds() > self.__TIME_TOLERANCE:
            
            logger.warning(f"No inflow_temp for cid = {house.customer_id}, sid = {house.subcentral_id} within 15 minutes before {now.isoformat()}")
            
        return data

    def get_data_for_building(self, house: models.House, time_range, now: pytz.datetime.datetime):
        
        now = now
        mintime = now - time_range[0]
        maxtime = now + time_range[1]
        logger.info(f"Fetching apartment data for cid = {house.customer_id}, sid = {house.subcentral_id} in time_range")

        query = self._APARTMENT_QUERY_TEMPLATE.format(customer_id = house.customer_id, subcentral_id = house.subcentral_id, \
                                                      mintime=mintime.strftime(self.__DATETIME_FORMAT), \
                                                      maxtime=maxtime.strftime(self.__DATETIME_FORMAT))

        print(query)

        dictionary ={"tstamp":[], "average_indoor_temperature":[]}

        rows = self.session.execute(query)
        
        if len(rows._current_rows) == 0:
                    
            raise ValueError(f"No initial indoor temperature is found for cid = {house.customer_id}, sid = {house.subcentral_id}, \
                            please check the data communication before starting MPC planning!")
       
        for row in rows:
            
            apartment_data = json.loads(row.json)
                           
            dictionary["tstamp"].append(apartment_data["tstamp_record"])
            dictionary["average_indoor_temperature"].append(apartment_data["sensor_temperature"])

            
        data = pd.DataFrame(dictionary)
        data.set_index("tstamp", inplace = True) 
        data.index = pd.to_datetime(data.index)
        data.sort_index(ascending = True, inplace = True)

#        data = data[data.index <= now]
        
        logger.warning(f"{data.average_indoor_temperature.isnull().sum()} null average_indoor_temperature out of {len(data)}")    
            
        valid_indexes = data.average_indoor_temperature.notnull().index[-1]
        valid_indexes = datetime.strptime(str(valid_indexes), "%Y-%m-%d %H:%M:%S%z")
        
        if (now - valid_indexes).total_seconds() > self.__TIME_TOLERANCE:
            
            logger.warning(f"No average_indoor_temperature for cid = {house.customer_id}, sid = {house.subcentral_id} within 15 minutes before {now.isoformat()}")

        return data
    
    # Add for flexibility service
    def get_plan_by_house(self, house: models.House, time_range, now: pytz.datetime.datetime):
                
        now = now
        mintime = now - time_range[0]
        maxtime = now + time_range[1]
        logger.debug(f"Fetching flexibility plan for cid = {house.customer_id}, sid = {house.subcentral_id} in time_range")

        query = self._FLEXIBILITY_PLAN_QUERY_TEMPLATE.format(customer_id = house.customer_id, subcentral_id = house.subcentral_id, \
                                                      mintime=mintime.strftime(self.__DATETIME_FORMAT), \
                                                      maxtime=maxtime.strftime(self.__DATETIME_FORMAT))

        print(query)

        dictionary ={"ts_start":[], "subcentral_plan":[]}

        rows = self.session.execute(query)
        
        if len(rows._current_rows) == 0:
                    
            logger.warning(f"No planned flexibility is found for cid = {house.customer_id}, sid = {house.subcentral_id}")
            data = pd.DataFrame(dictionary)
       
        else:
            for row in rows:          
                      
                schedules = json.loads(row.json)                
                dictionary["ts_start"].append(schedules["ts_start"])
                dictionary["subcentral_plan"].append(schedules["power_offset"]) 
                            
            data = pd.DataFrame(dictionary)
            data.set_index("ts_start", inplace = True) 
            data.index = pd.to_datetime(data.index)
            data.sort_index(ascending = True, inplace = True)
            logger.warning(f"{data.subcentral_plan.isnull().sum()} null subcentral_plan out of {len(data)}")    
      
        return data

    # Add for flexibility service
    def get_dispatch_by_house(self, house: models.House, time_range, now: pytz.datetime.datetime):
                
        now = now
        mintime = now - time_range[0]
        maxtime = now + time_range[1]
        logger.debug(f"Fetching dispatch plan for cid = {house.customer_id}, sid = {house.subcentral_id} in time_range")

        query = self._FLEXIBILITY_DISPATCH_QUERY_TEMPLATE.format(customer_id = house.customer_id, subcentral_id = house.subcentral_id, \
                                                      mintime=mintime.strftime(self.__DATETIME_FORMAT), \
                                                      maxtime=maxtime.strftime(self.__DATETIME_FORMAT))

        print(query)

        dictionary ={"ts_start":[], "subcentral_dispatch":[]}

        rows = self.session.execute(query)
        
        if len(rows._current_rows) == 0:
                    
            logger.warning(f"No dispatch plan is found for cid = {house.customer_id}, sid = {house.subcentral_id}")
            data = pd.DataFrame(dictionary)
       
        else:
            for row in rows:          
                      
                schedules = json.loads(row.json)                
                dictionary["ts_start"].append(schedules["ts_start"])
                dictionary["subcentral_dispatch"].append(schedules["power_offset"]) 
                            
            data = pd.DataFrame(dictionary)
            data.set_index("ts_start", inplace = True) 
            data.index = pd.to_datetime(data.index)
            data.sort_index(ascending = True, inplace = True)
            logger.warning(f"{data.subcentral_dispatch.isnull().sum()} null subcentral_dispatch out of {len(data)}")    
      
        return data
    
    def write_schedule_for_house(self, house: models.House, output: DataFrame, house_repo: HouseModelRepository):
        
        _, _, config, _, _ = house_repo.get_parameters_by_house(house)
        timestep = config.timestep
        
        logger.info(f"Writing schedule data for cid = {house.customer_id}, sid = {house.subcentral_id}")

        for index, row in output.iterrows():
            
            model_output = models.ModelOutput(                
            customer_id = house.customer_id,
            subcentral_id = house.subcentral_id,
            tstamp_record = datetime.now(pytz.timezone('UTC')).strftime(self.__DATETIME_FORMAT),
            ts_start = index.strftime(self.__DATETIME_FORMAT),
            ts_end = (index + timedelta(seconds = timestep)).strftime(self.__DATETIME_FORMAT),
            out_temp = row['out_temp_with_deviation'],
            baseline_power = row['baseline_power'],
            scheduled_power = row['power'],
            power_offset = row['power_offset'],
            inflow_temp_offset = row['inflow_temp_offset'],
            indoor_temp_estimate = row['indoor_temperature'],
            solar_irradiation = row['solar'],
            scheduled_inflow_temp = row['new_inflow_temp']
            )
                                   
            query = self._WRITE_QUERY_TEMPLATE.format(model_output_json = model_output.to_json())
               
            self.session.execute(query)
            
        logger.info("Finished writing schedules")
            
    def write_model_for_house(self, house: models.House, model_coef, model_intercept):
                
        logger.debug(f"Writing dynamic model for cid = {house.customer_id}, sid = {house.subcentral_id}")
           
        dynamic_model = models.DynamicIndoorModel(                
        customer_id = house.customer_id,
        subcentral_id = house.subcentral_id,
        tstamp_record = datetime.now(pytz.timezone('UTC')).strftime(self.__DATETIME_FORMAT),
        intercept = model_intercept,
        variable_coef = model_coef)
                   
        query = self._WRITE_MODEL_QUERY_TEMPLATE.format(dynamic_model_json = dynamic_model.to_json())
           
        self.session.execute(query)
            
        logger.debug("Finished writing dynamic model")         

    # Add for flexibility service
    def write_plan_for_house(self, house: models.House, output: DataFrame, house_repo: HouseModelRepository):
        
        _, _, config, _, _ = house_repo.get_parameters_by_house(house)
        timestep = config.timestep
        
        logger.info(f"Writing flexibility plan data for cid = {house.customer_id}, sid = {house.subcentral_id}")

        for index, row in output.iterrows():
            
            plan = models.Plan(                
            customer_id = house.customer_id,
            subcentral_id = house.subcentral_id,
            grid_zone = house.grid_zone,
            tstamp_record = datetime.now(pytz.timezone('UTC')).strftime(self.__DATETIME_FORMAT),
            ts_start = index.strftime(self.__DATETIME_FORMAT),
            ts_end = (index + timedelta(seconds = timestep)).strftime(self.__DATETIME_FORMAT),
            outside_temperature = row['out_temp_with_deviation'],
            heating_baseline = row['baseline_power'],
            heating_power = row['power'],
            power_offset = row['power_offset'],
            average_indoor_temperature = row['indoor_temperature']
            )
                                    
            query = self._WRITE_PLAN_QUERY_TEMPLATE.format(plan_json = plan.to_json())
               
            self.session.execute(query)
            
        logger.info("Finished writing plans")

    # Add for flexibility service
    def write_dispatch_for_house(self, house: models.House, output: DataFrame, house_repo: HouseModelRepository):
        
        _, _, config, _, _ = house_repo.get_parameters_by_house(house)
        timestep = config.timestep
        
        logger.info(f"Writing flexibility dispatch data for cid = {house.customer_id}, sid = {house.subcentral_id}")

        for index, row in output.iterrows():
            
            dispatch = models.Dispatch(                
            customer_id = house.customer_id,
            subcentral_id = house.subcentral_id,
            grid_zone = house.grid_zone,
            tstamp_record = datetime.now(pytz.timezone('UTC')).strftime(self.__DATETIME_FORMAT),
            ts_start = index.strftime(self.__DATETIME_FORMAT),
            ts_end = (index + timedelta(seconds = timestep)).strftime(self.__DATETIME_FORMAT),
            power_offset = row['subcentral_dispatch']
            )
                                   
            query = self._WRITE_DISPATCH_QUERY_TEMPLATE.format(dispatch_json = dispatch.to_json())
               
            self.session.execute(query)
            
        logger.info("Finished writing dispatch")

    # Add for flexibility service
    def write_report_for_house(self, house: models.House, output: DataFrame, utility_config: FlexibilityConfiguration):
        
        timestep = utility_config.timestep

        logger.info(f"Writing report data for cid = {house.customer_id}, sid = {house.subcentral_id}")

        for index, row in output.iterrows():
            
            report = models.Report(                
            customer_id = house.customer_id,
            subcentral_id = house.subcentral_id,
            grid_zone = house.grid_zone,
            tstamp_record = datetime.now(pytz.timezone('UTC')).strftime(self.__DATETIME_FORMAT),
            ts_start = index.strftime(self.__DATETIME_FORMAT),
            ts_end = (index + timedelta(seconds = timestep)).strftime(self.__DATETIME_FORMAT),
            power_offset = row['power_offset'],
            heating_baseline = row['baseline_power'],
            heating_power = row['heat_power'],
            average_indoor_temperature = row['average_indoor_temperature']
            )
                             
            query = self._WRITE_REPORT_QUERY_TEMPLATE.format(report_json = report.to_json())
               
            self.session.execute(query)
            
        logger.info("Finished writing report")  

# Read/write data related with weather                     
class CassandraWeatherRepository(WeatherRepository):

    _WEATHER_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.darksky_forecast
         WHERE location = '{location}'
            AND timestamp >= '{mintime}'
            AND timestamp <= '{maxtime}'
            ALLOW FILTERING
    '''
    _TEMP_DEVIATION_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.outside_temperature_forecast
         WHERE customer_id = {customer_id}
            AND subcentral_id = {subcentral_id}
            AND timestamp >= '{mintime}'
            AND timestamp <= '{maxtime}'
            ALLOW FILTERING
    '''
    
    _WRITE_QUERY_TEMPLATE = '''
        INSERT INTO flexheat.outside_temperature_forecast JSON 
        '{deviation_forecast_json}'
    ''' 
       
    __TIMEZONE = pytz.timezone('UTC')
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:00Z'

    def __init__(self, session):
        
        self.session = session
        
    def get_weather_by_location(self, house: models.House, time_range, now: pytz.datetime.datetime):
        
        now = now
        logger.info(f"Fetching weather data for {house.location} in time_range")

        mintime = now - time_range[0]
        maxtime = now + time_range[1]
        query = self._WEATHER_QUERY_TEMPLATE.format(location=house.location, \
                                                    mintime=mintime.strftime(self.__DATETIME_FORMAT), \
                                                    maxtime=maxtime.strftime(self.__DATETIME_FORMAT))

        dictionary ={"tstamp":[],"forecast_outside_temp":[], "forecast_cloud_cover":[]}
        print(query)

        rows = self.session.execute(query)
                
        if len(rows._current_rows) == 0:
                    
            raise ValueError(f"No weather forecast is found for cid = {house.customer_id}, sid = {house.subcentral_id}")
        
        for row in rows:
            
            weather_forecast = json.loads(row.json)
                        
            dictionary["tstamp"].append(weather_forecast["timestamp"])
            dictionary["forecast_outside_temp"].append(weather_forecast["temperature"])
            dictionary["forecast_cloud_cover"].append(weather_forecast["total_cloud_cover"])
            
        data = pd.DataFrame(dictionary)
        data.set_index("tstamp", inplace = True) 
        data.index = pd.to_datetime(data.index)
        data.sort_index(ascending = True, inplace = True)

        logger.warning(f"{data.forecast_outside_temp.isnull().sum()} null forecast_outside_temp out of {len(data)}")    
        logger.warning(f"{data.forecast_cloud_cover.isnull().sum()} null forecast_cloud_cover out of {len(data)}")    
        
        return data

    def get_temp_deviation_for_house(self, house: models.House, time_range, now: pytz.datetime.datetime):
        
        now = now
        logger.info(f"Fetching temp_deviation_for_house for {house.location} in time_range")

        mintime = now - time_range[0]
        maxtime = now + time_range[1]
        query = self._TEMP_DEVIATION_QUERY_TEMPLATE.format(customer_id = house.customer_id, subcentral_id = house.subcentral_id, \
                                                    mintime=mintime.strftime(self.__DATETIME_FORMAT), \
                                                    maxtime=maxtime.strftime(self.__DATETIME_FORMAT))

        dictionary ={"tstamp":[],"measure_forecast_deviation":[]}
        print(query)

        rows = self.session.execute(query)
                
        if len(rows._current_rows) == 0:
                    
            logger.info(f"No deviation forecast is found for cid = {house.customer_id}, sid = {house.subcentral_id}")
            
            data = pd.DataFrame(dictionary)
        
        else:
            for row in rows:
                
                forecast = json.loads(row.json)
                            
                dictionary["tstamp"].append(forecast["timestamp"])
                dictionary["measure_forecast_deviation"].append(forecast["measure_forecast_deviation"])
                
            data = pd.DataFrame(dictionary)
            data.set_index("tstamp", inplace = True) 
            data.index = pd.to_datetime(data.index)
            data.sort_index(ascending = True, inplace = True)
            logger.warning(f"{data.measure_forecast_deviation.isnull().sum()} null measure_forecast_deviation out of {len(data)}")    
        
        return data 
       
    def write_temp_deviation_for_house(self, house: models.House, forecast: DataFrame, house_repo: HouseModelRepository):
        
        _, _, config, _, _ = house_repo.get_parameters_by_house(house)
        timestep = config.timestep
        
        logger.info(f"Writing forecast outside temperature deviation for cid = {house.customer_id}, sid = {house.subcentral_id}")

        for index, row in forecast.iterrows():
            
            deviation_forecast = models.OutTempDeviationForecast(                
            customer_id = house.customer_id,
            subcentral_id = house.subcentral_id,
            timestamp = index.strftime(self.__DATETIME_FORMAT),
            measure_forecast_deviation = row['forecast_devaition'],
            tstamp_record = datetime.now(pytz.timezone('UTC')).strftime(self.__DATETIME_FORMAT)
            )
                    
            query = self._WRITE_QUERY_TEMPLATE.format(deviation_forecast_json = deviation_forecast.to_json())
               
            self.session.execute(query)
            
        logger.info("Finished writing forecast")

# Read/write data for subcentral/building and weather
class CassandraRepository(CassandraHouseRepository, CassandraWeatherRepository):
    
    def get_all_by_house(self, house, time_range, now):
        data = self.get_weather_by_location(house, time_range, now)
        data = data.join(self.get_temp_deviation_for_house(house, time_range, now), how = 'outer')
        data = data.join(self.get_data_for_subcentral(house, time_range, now), how = 'outer')
        data = data.join(self.get_data_for_building(house, time_range, now), how = 'outer')

        return data
    
    # Add for mpc with auto dynamic model 
    # Resample subcentral data (inflow_temp, measured_outside_temp) and building data (average_indoor_temperature) by timestep
    def get_all_by_house_resample(self, house, time_range, now, timestep):
        data = self.get_weather_by_location(house, time_range, now)        
        if not self.get_temp_deviation_for_house(house, time_range, now).empty:
            data = data.join(self.get_temp_deviation_for_house(house, time_range, now).resample(timedelta(seconds = timestep)).mean(), how = 'outer')

        data = data.join(self.get_data_for_subcentral(house, time_range, now).resample(timedelta(seconds = timestep)).mean(), how = 'outer')
        data = data.join(self.get_data_for_building(house, time_range, now).resample(timedelta(seconds = timestep)).first(), how = 'outer')

        return data

    # Add for mpc with auto dynamic model
    # Get subcentral data (inflow_temp) and building data (average_indoor_temperature) before planning start  
    def get_initial_by_house(self, house, time_range, now):
        data = self.get_data_for_subcentral(house, time_range, now)
        data = data.join(self.get_data_for_building(house, time_range, now), how = 'outer')

        return data

# Add for flexibility service
# Read/write data on aggregation level
class CassandraAggregateRepository():
    
    _GRID_PEAK_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.peak_hours
         WHERE customer_id = {customer_id}
            AND grid_zone = {grid_zone}
            AND ts_start >= '{mintime}'
            AND ts_start <= '{maxtime}'
            ALLOW FILTERING
    '''   
    
    _GRID_PLAN_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.aggregate_flexibility_plan
         WHERE customer_id = {customer_id}
            AND grid_zone = {grid_zone}
            AND ts_start >= '{mintime}'
            AND ts_start <= '{maxtime}'
            ALLOW FILTERING
    '''    
    _GRID_DISPATCH_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.aggregate_flexibility_dispatch
         WHERE customer_id = {customer_id}
            AND grid_zone = {grid_zone}
            AND ts_start >= '{mintime}'
            AND ts_start <= '{maxtime}'
            ALLOW FILTERING
    '''    
    
    _WRITE_PLAN_QUERY_TEMPLATE = '''
        INSERT INTO flexheat.aggregate_flexibility_plan JSON 
        '{output_json}'
    '''    
    
    _WRITE_REPORT_QUERY_TEMPLATE = '''
        INSERT INTO flexheat.aggregate_flexibility_report JSON 
        '{output_json}'
    '''   
    
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:00Z'
    
    def __init__(self, session):
        
        self.session = session 
        
    def get_peak_by_customer(self, customer, grid, time_range, now: pytz.datetime.datetime):
                
        now = now
        mintime = now - time_range[0]
        maxtime = now + time_range[1]
        logger.debug(f"Fetching peak hours for customer_id = {customer}, grid_zone = {grid}")

        query = self._GRID_PEAK_QUERY_TEMPLATE.format(customer_id = customer, \
                                                      grid_zone = grid, \
                                                      mintime=mintime.strftime(self.__DATETIME_FORMAT), \
                                                      maxtime=maxtime.strftime(self.__DATETIME_FORMAT))

        print(query)

        dictionary ={"ts_start":[], "ts_end":[]}

        rows = self.session.execute(query)
        
        if len(rows._current_rows) == 0:
                    
            logger.warning(f"No peak hour is found for cid = {customer}, grid_zone = {grid}")
            data = pd.DataFrame(dictionary)
       
        else:
            for row in rows:          
                      
                schedules = json.loads(row.json)                
                dictionary["ts_start"].append(schedules["ts_start"])
                dictionary["ts_end"].append(schedules["ts_end"]) 
                            
            data = pd.DataFrame(dictionary)
#            data.set_index("ts_start", inplace = True) 
#            data.index = pd.to_datetime(data.index)
#            data.sort_index(ascending = True, inplace = True)
#            data = data[data.index <= now]
            data['ts_start']= pd.to_datetime(data['ts_start']) 
            data['ts_end']= pd.to_datetime(data['ts_end']) 

        return data  
            
    def get_plan_by_customer(self, customer, grid, time_range, now: pytz.datetime.datetime):
                
        now = now
        mintime = now - time_range[0]
        maxtime = now + time_range[1]
        logger.debug(f"Fetching flexibility plan for customer_id = {customer}, grid_zone = {grid}")

        query = self._GRID_PLAN_QUERY_TEMPLATE.format(customer_id = customer, \
                                                      grid_zone = grid, \
                                                      mintime=mintime.strftime(self.__DATETIME_FORMAT), \
                                                      maxtime=maxtime.strftime(self.__DATETIME_FORMAT))

        print(query)

        dictionary ={"ts_start":[], "aggregate_plan":[]}

        rows = self.session.execute(query)
        
        if len(rows._current_rows) == 0:
                    
            logger.warning(f"No flexibility plan is found for cid = {customer}, grid_zone = {grid}")
            data = pd.DataFrame(dictionary)
       
        else:
            for row in rows:          
                      
                schedules = json.loads(row.json)                
                dictionary["ts_start"].append(schedules["ts_start"])
                dictionary["aggregate_plan"].append(schedules["power_offset"]) 
                            
            data = pd.DataFrame(dictionary)
            data.set_index("ts_start", inplace = True) 
            data.index = pd.to_datetime(data.index)
            data.sort_index(ascending = True, inplace = True)
            logger.warning(f"{data.aggregate_plan.isnull().sum()} null aggregate_plan out of {len(data)}")    
      
        return data           
    
    
    def get_dispatch_by_customer(self, customer, grid, time_range, now: pytz.datetime.datetime):
                
        now = now
        mintime = now - time_range[0]
        maxtime = now + time_range[1]
        logger.debug(f"Fetching dispatch plan for customer_id = {customer}, grid_zone = {grid}")

        query = self._GRID_DISPATCH_QUERY_TEMPLATE.format(customer_id = customer, \
                                                          grid_zone = grid, \
                                                          mintime=mintime.strftime(self.__DATETIME_FORMAT), \
                                                          maxtime=maxtime.strftime(self.__DATETIME_FORMAT))

        print(query)

        dictionary ={"ts_start":[], "aggregate_dispatch":[]}

        rows = self.session.execute(query)
        
        if len(rows._current_rows) == 0:
                    
            logger.warning(f"No dispatch is found for cid = {customer}, grid_zone = {grid}")
            data = pd.DataFrame(dictionary)
       
        else:
            for row in rows:          
                      
                schedules = json.loads(row.json)                
                dictionary["ts_start"].append(schedules["ts_start"])
                dictionary["aggregate_dispatch"].append(schedules["power_offset"]) 
                            
            data = pd.DataFrame(dictionary)
            data.set_index("ts_start", inplace = True) 
            data.index = pd.to_datetime(data.index)
            data.sort_index(ascending = True, inplace = True)
            logger.warning(f"{data.aggregate_dispatch.isnull().sum()} null powe_offset out of {len(data)}")    
      
        return data           
        
    def write_plan_for_grid(self, customer, grid, aggregate_repo: FlexibilityModelRepository, aggregate_plan: DataFrame):
        
        config = aggregate_repo.get_config_by_customer(customer)        
        timestep = config.timestep
        
        logger.info(f"Writing aggregate flexibility plan data for energy company customer_id = {customer}, grid_zone = {grid}")

        for index, row in aggregate_plan.iterrows():
            
            output = models.Flexibility(                                        
            customer_id = customer,
            grid_zone = grid,
            tstamp_record = datetime.now(pytz.timezone('UTC')).strftime(self.__DATETIME_FORMAT),
            ts_start = index.strftime(self.__DATETIME_FORMAT),
            ts_end = (index + timedelta(seconds = timestep)).strftime(self.__DATETIME_FORMAT),
            power_offset = row['aggregate_power_offset']
            )                         
            query = self._WRITE_PLAN_QUERY_TEMPLATE.format(output_json = output.to_json())
               
            self.session.execute(query)
            
        logger.info("Finished writing plans")   

    def write_report_for_grid(self, customer, grid, aggregate_repo: FlexibilityModelRepository, aggregate_report: DataFrame):
        
        config = aggregate_repo.get_config_by_customer(customer)        
        timestep = config.timestep
        
        logger.info(f"Writing aggregate flexibility report data for customer_id = {customer}, grid_zone = {grid}")

        for index, row in aggregate_report.iterrows():
            
            output = models.Flexibility(                                        
            customer_id = customer,
            grid_zone = grid,
            tstamp_record = datetime.now(pytz.timezone('UTC')).strftime(self.__DATETIME_FORMAT),
            ts_start = index.strftime(self.__DATETIME_FORMAT),
            ts_end = (index + timedelta(seconds = timestep)).strftime(self.__DATETIME_FORMAT),
            power_offset = row['aggregate_power_offset']
            )                         
            query = self._WRITE_REPORT_QUERY_TEMPLATE.format(output_json = output.to_json())
               
            self.session.execute(query)
            
        logger.info("Finished writing report")  
from db._cassandra import CassandraAggregateRepository
from db._models import HouseModelRepository, FlexibilityModelRepository
import pytz
from sklearn.linear_model import LinearRegression
import models
import math
import numpy as np
import pandas as pd
from datetime import timedelta
import logging

logger = logging.getLogger("__main__")

class AggregationPlan():
     
    def __init__(self, 
                 customer: int,
                 grid: int,
                 aggregate_repo: FlexibilityModelRepository,
                 flexibility_repo: CassandraAggregateRepository,
                 planning_start: pytz.datetime.datetime
                 ):
        
        self._customer = customer
        self._grid = grid
        self._aggregate_repo = aggregate_repo
        self._config = self._aggregate_repo.get_config_by_customer(self._customer)
        self._horizon = self._config.planning_horizon
        self._timestep = self._config.timestep
        self._flexibility_repo = flexibility_repo
        self._planning_start = planning_start
        self._time_range = self.get_time_range()
          
    # Get time range to define the time scale for planning
    def get_time_range(self):
        
        logger.info("Getting time range for flexibility planning:")        
      
        range_future = (self._horizon - 1) * self._timestep  
        time_range = [timedelta(seconds = 0), timedelta(seconds = range_future)]
        
        logger.info(f"time_range = {time_range}")
 
        return time_range

    # Fetch peak hours    
    def get_peak_hour(self):
         
#         index = pd.date_range(self._planning_start - self._time_range[0], self._planning_start + self._time_range[1], freq = f"{self._timestep}S")
#         data_planning = pd.DataFrame(index = index) 
#         data_planning['peak_hour'] = 0
           
        peak = self._flexibility_repo.get_peak_by_customer(
            customer = self._customer,
            grid = self._grid,
            time_range = self._time_range,
            now = self._planning_start
        )
                 
#         for index_all, row_all in data_planning.iterrows():
#             for index_peak, row_peak in peak.iterrows():
#                 if index_all >= row_peak['ts_start'] and index_all < row_peak['ts_end']:
#                     data_planning.at[index_all, 'peak_hour'] = 1  
             
#         return data_planning
        return peak
                
    # Calculate the aggregate flexibility by summarizing subcentral-level flexibility 
    def aggregate_plan(self, subcentral_plans):
        
        index = pd.date_range(self._planning_start - self._time_range[0], self._planning_start + self._time_range[1], freq = f"{self._timestep}S")
        data_planning = pd.DataFrame(index = index)
        
        if len(subcentral_plans) == 0:
            
            logger.warning("Subcentral-level flexibility is not available!")
            data_planning['aggregate_power_offset'] = 0
            
        else:    
            for subcentral in subcentral_plans:
                data_planning = pd.concat([data_planning, subcentral.power_offset.resample(timedelta(seconds = self._timestep)).mean()], axis = 1, join = 'outer')
            
            data_planning['aggregate_power_offset'] = data_planning.sum(axis = 1)
                
        return data_planning 
        
    def distribute_dispatch(self, subcentrals, cassandra_house_repo, house_repo):
        
        index = pd.date_range(self._planning_start - self._time_range[0], self._planning_start + self._time_range[1], freq = f"{self._timestep}S")
        data_planning = pd.DataFrame(index = index) 
          
        aggregate_dispatch = self._flexibility_repo.get_dispatch_by_customer(
            customer = self._customer,
            grid = self._grid,
            time_range = self._time_range,
            now = self._planning_start
        ).join(data_planning, how = 'outer')  

        aggregate_plan = self._flexibility_repo.get_plan_by_customer(
            customer = self._customer,
            grid = self._grid,
            time_range = self._time_range,
            now = self._planning_start
        ).join(data_planning, how = 'outer') 
        
        aggregate_plan.fillna(0, inplace=True)
                        
        subcentral_dispatches = []
        for subcentral in subcentrals:
            
            logger.info(f"Get plan for subcentral_id = {subcentral}")
            
            house = House(
                location=subcentral["geo_city"],
                customer_id=subcentral['customer_id'],
                subcentral_id=subcentral['subcentral_id'],
                longitude=subcentral['geo_coord_lon'],
                latitude=subcentral["geo_coord_lat"],
                grid_zone=subcentral["grid_zone"]
            )        
            
            subcentral_plan = cassandra_house_repo.get_plan_by_house(
                house = house, 
                time_range = self._time_range, 
                now = self._planning_start
                )
        
            if not subcentral_plan.empty:
                subcentral_plan.fillna(0, inplace=True)
                index = pd.date_range(self._planning_start - self._time_range[0], self._planning_start + self._time_range[1], freq = f"{self._timestep}S")
                subcentral_dispatch = pd.DataFrame(index = index)
                subcentral_dispatch = subcentral_dispatch.join(subcentral_plan, how = 'outer')         
                subcentral_dispatch = subcentral_dispatch.resample(timedelta(seconds = self._timestep)).mean()         
                
                subcentral_dispatch['subcentral_dispatch'] = np.nan                    
                for index, row in aggregate_plan.iterrows():
                    if row['power_offset'] != 0:
                        prop = subcentral_dispatch[index, 'subcentral_plan'] / row['aggregate_plan'] 
                        subcentral_dispatch['subcentral_dispatch'] = prop * aggregate_dispatch[index,'aggregate_dispatch']
                    else:
                        subcentral_dispatch['subcentral_dispatch'] = 0
        
                _, _, subcentral_config, _, _ = house_repo.get_parameters_by_house(house)
                subcentral_timestep = subcentral_config.timestep 
                subcentral_dispatch = subcentral_dispatch.resample(timedelta(seconds = subcentral_timestep)).mean()
                                                        
                logger.info(f"Write dispatch for subcentral = {subcentral}")
                
                cassandra_house_repo.write_dispatch_for_house(house = house,
                                                              output = subcentral_dispatch,
                                                              house_repo = house_repo)
                
                logger.info(f"Finished writing dispatch")            
            
            
                    
                
                
            

          
            
                     
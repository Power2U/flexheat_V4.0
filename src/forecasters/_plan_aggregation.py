from db._cassandra import CassandraAggregateRepository
from db._models import HouseModelRepository, FlexibilityModelRepository
import pytz
from sklearn.linear_model import LinearRegression
from models import *
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
                   
        peak = self._flexibility_repo.get_peak_by_customer(
            customer = self._customer,
            grid = self._grid,
            time_range = self._time_range,
            now = self._planning_start
        )
                 
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

        # Fetch aggregate dispatch and fill null values with 0       
        aggregate_dispatch = self._flexibility_repo.get_dispatch_by_customer(
            customer = self._customer,
            grid = self._grid,
            time_range = self._time_range,
            now = self._planning_start
        ).join(data_planning, how = 'outer')  
        
        aggregate_dispatch.fillna(0, inplace=True)
        
        # Fetch aggregate plan and fill null values with 0        
        aggregate_plan = self._flexibility_repo.get_plan_by_customer(
            customer = self._customer,
            grid = self._grid,
            time_range = self._time_range,
            now = self._planning_start
        ).join(data_planning, how = 'outer') 
        
        aggregate_plan.fillna(0, inplace=True)
        
        # Calculate dispatch for each subcentral             
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
        
            if subcentral_plan.empty:
                continue
            else:
                subcentral_plan.fillna(0, inplace=True)
                index = pd.date_range(self._planning_start - self._time_range[0], self._planning_start + self._time_range[1], freq = f"{self._timestep}S")
                subcentral_dispatch = pd.DataFrame(index = index)
                subcentral_dispatch = subcentral_dispatch.join(subcentral_plan, how = 'outer')         
                subcentral_dispatch = subcentral_dispatch.resample(timedelta(seconds = self._timestep)).mean()                         
                subcentral_dispatch['subcentral_dispatch'] = 0  
                                  
                for index, row in aggregate_plan.iterrows():
                    if row['aggregate_plan'] != 0:
                        prop = subcentral_dispatch.loc[index, 'subcentral_plan'] / row['aggregate_plan'] 
                        
                        # If aggregate plan and dispatch have different direction, consider dispatch as 0
                        if row['aggregate_plan'] * aggregate_dispatch.loc[index,'aggregate_dispatch'] < 0:
                            logger.warning(f"dispatch and plan have different sign at {index}")
                            subcentral_dispatch.loc[index, 'subcentral_dispatch'] = 0
                        # If aggregate plan and dispatch have the same direction, dispatch for each subcentral is in proportion to its plan                    
                        else:
                            subcentral_dispatch.loc[index, 'subcentral_dispatch'] = prop * aggregate_dispatch.loc[index,'aggregate_dispatch']
                    else:
                        subcentral_dispatch['subcentral_dispatch'] = 0
        
                _, _, subcentral_config, _, _ = house_repo.get_parameters_by_house(house)
                subcentral_timestep = subcentral_config.timestep 
                subcentral_dispatch = subcentral_dispatch.resample(timedelta(seconds = subcentral_timestep)).mean()
                                                        
                logger.info(f"Write dispatch for subcentral = {subcentral}")
                
                cassandra_house_repo.write_dispatch_for_house(house = house,
                                                              output = subcentral_dispatch,
                                                              house_repo = house_repo)
                            
            
                    
                
                
            

          
            
                     

from db._base import HouseModelRepository
import models
from mpc.params import *
from mpc.greybox_execution import PnPkModel_Execution
from mpc.greybox_plan import PnPkModel_Plan
import typing
import json
import logging

logger = logging.getLogger("__main__")
 
class RESTHouseModelRepository(HouseModelRepository):

    _PARAMETER_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.model_parameters
         WHERE customer_id = {customer_id}
            AND subcentral_id = {subcentral_id}
            AND active = {active}
            LIMIT 1
            ALLOW FILTERING
    '''
           
    _HEATCURVE_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.subcentral_heatcurve
         WHERE customer_id = {customer_id}
            AND subcentral_id = {subcentral_id}
            AND active = {active}
            ALLOW FILTERING
    '''    

    _MODEL_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.dynamic_indoor_temperature_model
         WHERE customer_id = {customer_id}
            AND subcentral_id = {subcentral_id}
            LIMIT 1
            ALLOW FILTERING
    '''    
         
    def __init__(self, session):
        
        self.session = session

    # Add for auto dynamic model
    # dynamic_config and dynamic_model are added as inputs 
    # Revised name for flexibility plan
    def get_model_by_house_plan(self, house: models.House):
        
        phys, sim, mpc_config, sarimax, dynamic_config = self.get_parameters_by_house(house)
        
        dynamic_model = self.get_dynamic_model_by_house(house)

        return PnPkModel_Plan(physical=phys, mpc_config=mpc_config, dynamic_config=dynamic_config, dynamic_model=dynamic_model)
    
    # Add for flexibility service execution
    def get_model_by_house_execution(self, house: models.House):
        
        phys, sim, mpc_config, sarimax, dynamic_config = self.get_parameters_by_house(house)
        
        dynamic_model = self.get_dynamic_model_by_house(house)

        return PnPkModel_Execution(physical=phys, mpc_config=mpc_config, dynamic_config=dynamic_config, dynamic_model=dynamic_model) 
    
    def get_parameters_by_house(self, house: models.House):
        
        logger.info(f"Fetching model parameters for cid = {house.customer_id}, sid = {house.subcentral_id}")

        query = self._PARAMETER_QUERY_TEMPLATE.format(customer_id = house.customer_id, \
                                                      subcentral_id = house.subcentral_id, \
                                                      active=True)

        print(query)
        
        rows = self.session.execute(query)
               
        if len(rows._current_rows) == 0:
                    
            # If there is no model parameters in database, default values will be used
            logger.warning(f"No active model parameters is found for cid = {house.customer_id}, sid = {house.subcentral_id} in database,\
                           default values in params.py will be used.")
        
        # The query returns the first row 
        # If there are more than one active records, the latest active parameters are adopted
        for row in rows:
            
            parameters = json.loads(row.json)
            
            physical_house = PhysicalHouseParameters(parameters)

            grey_params = HouseSimulationParameters(parameters)
      
            mpc_config = MPCConfiguration(parameters)
            
            sarimax_config = SARIMAXConfiguration(parameters)
            
            dynamic_config = DynamicConfiguration(parameters)
            
        return physical_house, grey_params, mpc_config, sarimax_config, dynamic_config
    
    def get_heatcurve_by_house(self, house: models.House, planning_start):

        logger.info(f"Fetching heat curve for cid = {house.customer_id}, sid = {house.subcentral_id}")

        query = self._HEATCURVE_QUERY_TEMPLATE.format(customer_id = house.customer_id, \
                                                      subcentral_id = house.subcentral_id, \
                                                      active=True)

        print(query)
        
        rows = self.session.execute(query)
        
        if len(rows._current_rows) == 0:
                    
            raise ValueError(f"No heat curve is found for cid = {house.customer_id}, sid = {house.subcentral_id}")  
              
        count = 0
        heatcurve_group = []
               
        for row in rows:
            
            heat_curve = json.loads(row.json)
                       
            # There can be different heat curves in different season
            # Compare current month with valid months
            # If there are more than one active curves, the latest active curve is adopted
            valid_start = heat_curve["valid_start_month"]
            valid_end = heat_curve["valid_until_month"]
            
            if valid_start <= valid_end:
                
                valid_month = list(range(valid_start, valid_end + 1))
                
            else:
                
                valid_month = list(range(valid_start, 13)) + list(range(1, valid_end + 1))
            
            if planning_start.month not in valid_month:
                
                continue
            
            else:
                           
                heatcurve = models.HeatCurve(    
                break_point = heat_curve["break_point_number"],
                out_temp = heat_curve["outside_temperature"],
                inflow_temp = heat_curve["inflow_temperature"],
                power = heat_curve["heating_power"]                
                )
                
                heatcurve_group.append(heatcurve)
                count += 1

        if count == 0:
            raise ValueError(f"No heat curve is found for {house.customer_id, house.subcentral_id} for month {planning_start.month}, \
                            please config the heat curve first!")
        
        elif count > 1:
            logger.warning(f"More than one active heat curve is found for {house.customer_id, house.subcentral_id} for month {planning_start.month}, \
                            the latest one will be used.")
                    
        return heatcurve_group[0] # Return the latest valid heat curve
        
    def get_dynamic_model_by_house(self, house: models.House):

        logger.debug(f"fetching dynamic indoor temperature model for cid = {house.customer_id}, sid = {house.subcentral_id}")
        
        query = self._MODEL_QUERY_TEMPLATE.format(customer_id = house.customer_id,\
                                                  subcentral_id = house.subcentral_id)

        print(query)
        
        rows = self.session.execute(query)
        
        if len(rows._current_rows) == 0:
            
            # If there is no model parameters in database, default values will be used                    
            logger.warning(f"No dynamic model is found for cid = {house.customer_id}, sid = {house.subcentral_id} in database,\
                            default values in params.py will be used.")  
              
        # The query returns the first row 
        # If there are more than one active records, the latest active parameters are adopted               
        for row in rows:
            
            parameters = json.loads(row.json)
            
            dynamic_model = DynamicModel(parameters)
            
        return dynamic_model  

# Add for flexibility service    
class FlexibilityModelRepository():
    
    _CONFIG_QUERY_TEMPLATE = '''
        SELECT JSON * FROM flexheat.flexibility_config
         WHERE customer_id = {customer_id}
            ALLOW FILTERING
    '''
         
    def __init__(self, session):
        
        self.session = session
        
    def get_config_by_customer(self, customer):
        
        logger.info(f"Fetching flexibility config parameters for cid = {customer}")

        query = self._CONFIG_QUERY_TEMPLATE.format(customer_id = customer)

        print(query)
        
        rows = self.session.execute(query)
               
        if len(rows._current_rows) == 0:
                    
            # If there is no config parameters in database, default values will be used
            logger.warning(f"No active config parameters is found for cid = {customer} in database,\
                           default values in params.py will be used.")
        
        for row in rows:
            
            parameters = json.loads(row.json)    
            flexibility_config = FlexibilityConfiguration(parameters)
                        
        return flexibility_config
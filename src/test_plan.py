import logging
import os
import json
from elasticsearch import Elasticsearch
from datetime import *
from db import *
from forecasters._forecast_flexibility import HouseDataForecaster
from forecasters._plan_aggregation import AggregationPlan
from regulators._mpc_flexibility_plan import MPCController_Plan
from models import *
from deploy_utils import *
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0
    Step 1.3 in the 3-step service delivery model
    Planning on subcentral- and aggregation-level

'''

# Subcentral-level planning, estimate flexibility from each subcentral        
def runSubcentralForecaster(house, cassandra_repo, house_repo, planning_start, grid_peak):
    logger.info("start forecaster for house")
    logger.info(f"{house}")

    try:
        forecaster = HouseDataForecaster(
            house=house,
            cassandra_repo=cassandra_repo,
            house_repo=house_repo,
            planning_start=planning_start,
            grid_peak = grid_peak
        )

        controller = MPCController_Plan(forecaster)

        output = controller.control()

        cassandra_repo.write_plan_for_house(house, output, house_repo)
        
        return output

    except ValueError as ve:
        logger.error(ve)

    except Exception as ex:
        logger.error(ex)

# Aggregation-level planning, estimate aggregate flexibility in the grid        
def runGridForecaster(customer, grid, aggregate_repo, planning_start, subcentral_plans, flexibility_repo):
    logger.info(f"Aggregate flexibility for energy company customer_id = {customer}, grid_zone = {grid}")

    try:
        forecaster = AggregationPlan(
            customer = customer,
            grid = grid,
            aggregate_repo = aggregate_repo,
            flexibility_repo = flexibility_repo,
            planning_start = planning_start
        )
        
        aggregate_plan = forecaster.aggregate_plan(subcentral_plans = subcentral_plans)
        flexibility_repo.write_plan_for_grid(customer, grid, aggregate_repo, aggregate_plan)

    except ValueError as ve:
        logger.error(ve)

    except Exception as ex:
        logger.error(ex)

        
def lambda_handler():  

    DB_URL = '13.48.110.27'
    if "DB_URL" in os.environ:
        DB_URL = os.environ['DB_URL']

    dbConnection = DBConnection(DB_URL)
    session = dbConnection.session

    cassandra_repo = CassandraRepository(session)
    house_repo = RESTHouseModelRepository(session)
    aggregate_repo = FlexibilityModelRepository(session)
    flexibility_repo = CassandraAggregateRepository(session)

    planning_start = "2020-10-01 00:00:00.000Z"
    planning_start = datetime.strptime(planning_start, "%Y-%m-%d %H:%M:%S.%f%z")    
 
    logger.info(f"Planning to start: {planning_start}")
      
    ES_URL = 'http://13.48.110.27:9200/'    
    es = connectES(ES_URL)    
    
    utilities = getActiveUtility(es)  
       
    logger.info("Active energy companies:")
    
    logger.info(f"customer_id: {utilities}")
        
    for utility in utilities:
        
        logger.info(f"Plan for energy company customer_id = {utility}")
       
        grids = getActiveGrid(es, utility, planning_start)
        
        if bool(grids) is False:
            logger.info("No peak hours are specified")
            continue 
                       
        logger.info(f"Flexibility is needed for grid_zone: {grids}")
              
        for grid in grids:
            
            subcentral_plans = []
        
            logger.info(f"Plan for grid_zone = {grid}")
            
            grid_peak = runGridPeak(utility, grid, aggregate_repo, flexibility_repo, planning_start)
            
            subcentrals = getActiveSubcentrals(es, utility, grid)
            
            if len(subcentrals) == 0:
                logger.info("No active subcentral is found")
                continue
            
            logger.info(f"Active subcentrals are: {subcentrals}")
  
            for subcentral in subcentrals:
                
                logger.info(f"Plan for subcentral = {subcentral}")
                
                house = House(
                    location=subcentral["geo_city"],
                    customer_id=subcentral['customer_id'],
                    subcentral_id=subcentral['subcentral_id'],
                    longitude=subcentral['geo_coord_lon'],
                    latitude=subcentral["geo_coord_lat"],
                    grid_zone=subcentral["grid_zone"]
                )
                subcentral_plan = runSubcentralForecaster(house, cassandra_repo, house_repo, planning_start, grid_peak)
                
                subcentral_plans.append(subcentral_plan)
                
            runGridForecaster(utility, grid, aggregate_repo, planning_start, subcentral_plans, flexibility_repo)
        
    dbConnection.db_shutdown()

    return json.dumps({})

if __name__ == '__main__':
    lambda_handler()


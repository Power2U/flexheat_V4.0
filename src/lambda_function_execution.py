import logging
import os
import json
from elasticsearch import Elasticsearch
from datetime import *
from db import *
from forecasters._forecast_flexibility import HouseDataForecaster
from forecasters._plan_aggregation import AggregationPlan
from models import *
from regulators._mpc_flexibility_execution import MPCController_Execution
from deploy_utils import *

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0
    Step 2.2 in the 3-step service delivery model
    Determine schedules for subcentrals

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

        controller = MPCController_Execution(forecaster)

        output = controller.control()

        cassandra_repo.write_schedule_for_house(house, output, house_repo)
        
        return output

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
#     start = datetime.utcnow() + timedelta(hours=1)
#     start = start.strftime("%Y-%m-%d %H:00:00.000Z")
#    planning_start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S.%f%z")    
    logger.info(f"Planning to start: {planning_start}")
      
    ES_URL = 'http://13.48.110.27:9200/'    
    es = connectES(ES_URL)    
    
    utilities = getActiveUtility(es)  
       
    logger.info("Active energy companies:")
    
    logger.info(f"customer_id: {utilities}")
        
    for utility in utilities:
        
        logger.info(f"Plan for customer_id = {utility}")
       
        grids = getActiveGrid(es, utility, planning_start) # return peak hours sorting by grid zone
        
        logger.info(f"Flexibility is needed for grid_zone = {grids}")
       
        for grid in grids:
        
            logger.info(f"Plan for grid_zone = {grid}")
            
            grid_peak = runGridPeak(utility, grid, aggregate_repo, flexibility_repo, planning_start)
            
            subcentrals = getActiveSubcentrals(es, utility, grid)# return subcentrals in a grid zone
            
            logger.info(f"Active subcentrals are: {subcentrals}")
  
            for subcentral in subcentrals:
                
                logger.info(f"Plan for subcentral_id = {subcentral}")
                
                house = House(
                    location=subcentral["geo_city"],
                    customer_id=subcentral['customer_id'],
                    subcentral_id=subcentral['subcentral_id'],
                    longitude=subcentral['geo_coord_lon'],
                    latitude=subcentral["geo_coord_lat"],
                    grid_zone=subcentral["grid_zone"]
                )
                runSubcentralForecaster(house, cassandra_repo, house_repo, planning_start, grid_peak)
        
    dbConnection.db_shutdown()

    return json.dumps({})

if __name__ == '__main__':
    lambda_handler()


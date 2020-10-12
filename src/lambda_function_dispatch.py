import logging
import os
import json
from elasticsearch import Elasticsearch
from datetime import *
from db import *
from forecasters._forecast_flexibility import HouseDataForecaster
from forecasters._plan_aggregation import AggregationPlan
from models import *
from deploy_utils import *

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0
    Step 2.1 in the 3-step service delivery model
    Distribute dispatch orders for subcentrals

'''

def runGridDispatch(utility, grid, aggregate_repo, flexibility_repo, planning_start, subcentrals, cassandra_house_repo, house_repo):
    logger.info(f"Dispatch flexibility for customer_id = {utility}, grid_zone = {grid}")

    try:
        forecaster = AggregationPlan(
            customer = utility,
            grid = grid,
            aggregate_repo = aggregate_repo,
            flexibility_repo = flexibility_repo,
            planning_start = planning_start
        )
        
        subcentral_dispatches = forecaster.distribute_dispatch(subcentrals = subcentrals, 
                                                           cassandra_house_repo = cassandra_house_repo,
                                                           house_repo = house_repo
                                                           )       

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

    cassandra_house_repo = CassandraHouseRepository(session)
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
        
        logger.info(f"Dispatch for energy company customer_id = {utility}")
       
        grids = getActiveGrid(es, utility, planning_start)
        
        if bool(grids) is False:
            logger.info("No peak hours have been specified")
            continue 
                       
        logger.info(f"Flexibility is needed for grid_zone: {grids}")
       
        for grid in grids:
            
            logger.info(f"Dispatch for grid_zone = {grid}")
            
            subcentrals = getActiveSubcentrals(es, utility, grid)
            
            logger.info(f"Active subcentrals are: {subcentrals}")
  
            runGridDispatch(utility, grid, aggregate_repo, flexibility_repo, planning_start, subcentrals, cassandra_house_repo, house_repo)
            
    dbConnection.db_shutdown()
    
    return json.dumps({})

if __name__ == '__main__':
    lambda_handler()

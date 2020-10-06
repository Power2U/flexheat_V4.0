import logging
import os
import json
from elasticsearch import Elasticsearch
from datetime import *
from db import *
from forecasters._forecast_flexibility import HouseDataForecaster
from forecasters._plan_aggregation import AggregationPlan
from models import *

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0
    Step 2.1 in the 3-step service delivery model
    Distribute dispatch orders for subcentrals

'''

def connectES(ES_URL):
    
    if "ES_URL" in os.environ:
        ES_URL = os.environ['ES_URL']

    es = Elasticsearch([ES_URL])  
    
    return es  

def getActiveUtility(es):

    res = es.search(index="flexheat_customers", body={"query": {"bool": {"must": [{"term": {
                    "enable_flexibility": "true"}}], "must_not": [], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}})

    result = []

    for item in res['hits']['hits']:
        result.append(item["_source"])

    return result

def getActiveGrid(es, utility, planning_start):

    # TODO: fetch peak hours: ts_start>= planning_start and customer_id = utility
    res = es.search(index="aggregate_flexibility_dispatch", body={"query": {"bool": {"must": [{"term": {
                    "customer_id": "utility"}}], "must_not": [], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}})

    result = {} # set, without duplicate values

    for item in res['hits']['hits']:
        result.add(item["_source"]["grid_zone"]) # return grid_zone 

    return result

def getActiveSubcentrals(es, utility, grid): 

    # TODO: fetch subcentrals: customer_id = utility, grid_zone = grid, enable_fcc = true, enable_flexibility = true
    res = es.search(index="flexheat_peak_hours", body={"query": {"bool": {"must": [{"term": {
                    "enable_flexibility": "true"}}], "must_not": [], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}})

    result = []

    for item in res['hits']['hits']:
        result.append(item["_source"]) # return subcentral record

    return result

def runGridDispatch(utility, grid, aggregate_repo, flexibility_repo, start, subcentrals, cassandra_house_repo):
    logger.info(f"Dispatch flexibility for customer_id = {utility}, grid_zone = {grid}")

    try:
        forecaster = AggregationPlan(
            customer = utility,
            grid = grid,
            aggregate_repo = aggregate_repo,
            flexibility_repo = flexibility_repo,
            planning_start = start
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
    flexibility_repo = CassandraFlexibilityRepository(session)

    planning_start = datetime.utcnow() + timedelta(hours=1)

    planning_start = planning_start.strftime("%Y-%m-%d %H:00:00.000Z")

    logger.info(f"Dispatch to start: {planning_start}")
    
    start = datetime.strptime(planning_start, "%Y-%m-%d %H:%M:%S.%f%z")
  
    ES_URL = 'http://13.48.110.27:9200/'
    
    es = connectES(ES_URL)
    
    utilities = getActiveUtility(es) 
    
    logger.info("Active energy companies:")
    logger.info(f"customer_id = {utilities}")
        
    for utility in utilities:
        
        logger.info(f"Dispatch for energy company customer_id = {utility}")
       
        grids = getActiveGrid(es, utility, planning_start) # return peak hours sorting by grid zone
        
        logger.info(f"Flexibility is needed for grid_zone = {grids}")
       
        for grid in grids:
            
            logger.info(f"Dispatch for grid_zone = {grid}")
            
            subcentrals = getActiveSubcentrals(es, utility, grid)# return subcentrals in a grid zone
            
            logger.info(f"Active subcentrals are: {subcentrals}")
  
            runGridDispatch(utility, grid, aggregate_repo, flexibility_repo, start, subcentrals, cassandra_house_repo, house_repo)
            
    dbConnection.db_shutdown()
    
    return json.dumps({})

if __name__ == '__main__':
    lambda_handler()

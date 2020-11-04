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

# Fetch all subcentrals that enable the flexibility service and optimization (fcc)
# All subcentrals enabling flexibility service and optimization will be scheduled
# If a subcentral only enables optimization, flexheat V3.0 would be applied instead in other microservice
def getActiveSubcentrals_execution(es, utility): 
    
    # Fetch all customers supplied by the energy company
    res = es.search(index="flexheat_customers", body={"query":{"bool":{"must":[{"term":{"customer_parent":f"{utility}"}}],
        "must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"aggs":{}})

    customers = []

    for item in res['hits']['hits']:
        customers.append(item["_source"]["customer_id"])
    
    # Fetch all subcentrals belonging to the customer
    subcentrals = []

    for customer in customers:
        res = es.search(index="flexheat_subcentral", body={"query":{"bool":{"must":[{"term":{"customer_id":f"{customer}"}},
            {"term":{"enable_fcc":"true"}},{"term":{"enable_flex":"true"}}],"must_not":[],"should":[]}},
            "from":0,"size":50,"sort":[],"aggs":{}})        

        for item in res['hits']['hits']:
            subcentrals.append(item["_source"])
        
    return subcentrals

# Subcentral-level planning, generate execution schedule for each subcentral     
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
        
def lambda_handler(planning_start):  

    DB_URL = '13.48.110.27'
    if "DB_URL" in os.environ:
        DB_URL = os.environ['DB_URL']

    dbConnection = DBConnection(DB_URL)
    session = dbConnection.session

    cassandra_repo = CassandraRepository(session)
    house_repo = RESTHouseModelRepository(session)
    aggregate_repo = FlexibilityModelRepository(session)
    flexibility_repo = CassandraAggregateRepository(session)
     
    ES_URL = 'http://13.48.110.27:9200/'    
    es = connectES(ES_URL)    
    
    utilities = getActiveUtility(es)  
       
    logger.info("Active energy companies:")
    
    logger.info(f"customer_id: {utilities}")
        
    for utility in utilities:
        
        logger.info(f"Plan for customer_id = {utility}")
    
        subcentrals = getActiveSubcentrals_execution(es, utility)# return subcentrals belonging to the utility      
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
            
            grid_peak = runGridPeak(utility, subcentral["grid_zone"], aggregate_repo, flexibility_repo, planning_start)
            runSubcentralForecaster(house, cassandra_repo, house_repo, planning_start, grid_peak)
        
    dbConnection.db_shutdown()

    return json.dumps({})

if __name__ == '__main__':
    start = datetime.utcnow() + timedelta(hours=1)
    start = start.strftime("%Y-%m-%d %H:00:00.000Z")
    planning_start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S.%f%z")    
    logger.info(f"Planning to start: {planning_start}")    
    lambda_handler(planning_start)


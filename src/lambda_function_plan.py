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

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0
    Step 1.3 in the 3-step service delivery model
    Planning on subcentral- and aggregation-level

'''

def connectES(ES_URL):
    
    if "ES_URL" in os.environ:
        ES_URL = os.environ['ES_URL']

    es = Elasticsearch([ES_URL])  
    
    return es  

# Fetch all energy companies that enable the flexibility service
def getActiveUtility(es):

    res = es.search(index="flexheat_customers", body={"query":{"bool":{"must":[{"term":{"enable_flex":"true"}},{"term":{"customer_parent":"0"}}],
        "must_not":[],"should":[]}},"from":0,"size":50,"sort":[],"aggs":{}})

    result = []

    for item in res['hits']['hits']:
        result.append(item["_source"]["customer_id"])

    return result

# Fetch all grid zones where peak hours are specified. 
# Such grid zones need flexibility.
def getActiveGrid(es, utility, planning_start):

    start = planning_start.strftime('%Y-%m-%dT%H:%M:%SZ')
    res = es.search(index="flexheat_peak_hours", body={"query":{"bool":{"must":[{"term":{"customer_id":f"{utility}"}},
        {"range":{"ts_start":{"gt":start}}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"aggs":{}})

    result = set() # set, without duplicate values

    for item in res['hits']['hits']:
        result.add(item["_source"]["grid_zone"]) # return grid_zone 

    return result

# Fetch all subcentrals that enable the flexibility service and optimization.
def getActiveSubcentrals(es, utility, grid): 
    
    # Fetch all customers supplied by the energy company
    res = es.search(index="flexheat_customers", body={"query":{"bool":{"must":[{"term":{"customer_parent":f"{utility}"}}],
        "must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"aggs":{}})

    customers = []

    for item in res['hits']['hits']:
        customers.append(item["_source"]["customer_id"])
    
    # Fetch all subcentrals belonging to the customer and locate in the grid zone
    subcentrals = []

    for customer in customers:
        res = es.search(index="flexheat_subcentral", body={"query":{"bool":{"must":[{"term":{"customer_id":f"{customer}"}},
            {"term":{"grid_zone":f"{grid}"}},{"term":{"enable_fcc":"true"}},{"term":{"enable_flex":"true"}}],"must_not":[],"should":[]}},
            "from":0,"size":10,"sort":[],"aggs":{}})        

        for item in res['hits']['hits']:
            subcentrals.append(item["_source"])
        
    return subcentrals

# Process peak hours for planning
def runGridPeak(utility, grid, aggregate_repo, flexibility_repo, planning_start):
    logger.info(f"Get peak hours for customer_id = {utility}, grid_zone = {grid}")

    try:
        forecaster = AggregationPlan(
            customer = utility,
            grid = grid,
            aggregate_repo = aggregate_repo,
            flexibility_repo = flexibility_repo,
            planning_start = planning_start
        )
        
        grid_peak = forecaster.get_peak_hour()
        
        return grid_peak

    except ValueError as ve:
        logger.error(ve)

    except Exception as ex:
        logger.error(ex)

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

#     planning_start = "2020-10-01 00:00:00.000Z"
#     planning_start = datetime.strptime(planning_start, "%Y-%m-%d %H:%M:%S.%f%z")    
    start = datetime.utcnow() + timedelta(hours=1)
    start = start.strftime("%Y-%m-%d %H:00:00.000Z")
    planning_start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S.%f%z")    
    logger.info(f"Planning to start: {planning_start}")
      
    ES_URL = 'http://13.48.110.27:9200/'    
    es = connectES(ES_URL)    
    
    utilities = getActiveUtility(es)  
       
    logger.info("Active energy companies:")
    
    logger.info(f"customer_id: {utilities}")
        
    for utility in utilities:
        
        logger.info(f"Plan for energy company customer_id = {utility}")
       
        grids = getActiveGrid(es, utility, planning_start) # return peak hours sorting by grid zone
        
        if bool(grids) is False:
            logger.info("No peak hours are specified")
            continue 
                       
        logger.info(f"Flexibility is needed for grid_zone: {grids}")
              
        for grid in grids:
            
            subcentral_plans = []
        
            logger.info(f"Plan for grid_zone = {grid}")
            
            grid_peak = runGridPeak(utility, grid, aggregate_repo, flexibility_repo, planning_start)
            
            subcentrals = getActiveSubcentrals(es, utility, grid)# return subcentrals in a grid zone
            
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


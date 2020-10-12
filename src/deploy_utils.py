import logging
import os
import json
from elasticsearch import Elasticsearch
from forecasters._plan_aggregation import AggregationPlan

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

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
        

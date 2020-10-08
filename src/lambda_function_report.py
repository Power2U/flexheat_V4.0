import logging
import os
import json
from elasticsearch import Elasticsearch
from datetime import *
from db import *
from report._subcentral_report import SubcentralReport
from report._aggregate_report import AggregationReport
from models import *

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0
    Step 3.1 and 3.2 in the 3-step service delivery model
    Report the actually delivered flexibility on subcentral- and aggregation-level

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

def runSubcentralReporter(house, cassandra_repo, house_repo, report_start, utility_config):
    logger.info("start reporter for house")
    logger.info(f"{house}")

    try:
        reporter = SubcentralReport(
            house=house,
            cassandra_repo=cassandra_repo,
            house_repo=house_repo,
            report_start=report_start,
            utility_config=utility_config
        )

        subcentral_report = reporter.report

        cassandra_repo.write_report_for_house(house, subcentral_report, utility_config)
        
        return subcentral_report

    except ValueError as ve:
        logger.error(ve)

    except Exception as ex:
        logger.error(ex)

def runGridReporter(customer, grid, aggregate_repo, report_start, subcentral_reports, flexibility_repo):
    logger.info(f"Aggregate flexibility for customer_id = {customer}, grid_zone = {grid}")

    try:
        reporter = AggregationReport(
            customer = customer,
            grid = grid,
            aggregate_repo = aggregate_repo,
            flexibility_repo = flexibility_repo,
            report_start = report_start
        )
                
        aggregate_report = reporter.aggregate_report(subcentral_reports = subcentral_reports)
        
        flexibility_repo.write_report_for_grid(customer, grid, aggregate_repo, aggregate_report)

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

    report_start = "2020-10-01 00:00:00.000Z" 
    report_start = datetime.strptime(report_start, "%Y-%m-%d %H:%M:%S.%f%z")    
 
#     report_start = datetime.utcnow() - timedelta(hours=24)
#     report_start = report_start.strftime("%Y-%m-%d %H:00:00.000Z")
#     report_start = datetime.strptime(report_start, "%Y-%m-%d %H:%M:%S.%f%z")

    logger.info(f"Report for the period from: {report_start}")
    
    ES_URL = 'http://13.48.110.27:9200/'
    
    es = connectES(ES_URL)
    
    utilities = getActiveUtility(es) 
    
    logger.info("Active energy companies:")
    logger.info(f"customer_id = {utilities}")
        
    for utility in utilities:
        
        logger.info(f"Report for customer_id = {utility}")
        
        utility_config = aggregate_repo.get_config_by_customer(utility)
        
        grids = getActiveGrid(es, utility, report_start)
        
        logger.info(f"Flexibility is needed for grid_zone = {grids}")
       
        for grid in grids:
            
            subcentral_reports = []
        
            logger.info(f"Report for grid_zone = {grid}")
            
            subcentrals = getActiveSubcentrals(es, utility, grid)
            
            logger.info(f"Active subcentrals are: {subcentrals}")
  
            for subcentral in subcentrals:
                
                logger.info(f"Report for subcentral = {subcentral}")
                
                house = House(
                    location=subcentral["geo_city"],
                    customer_id=subcentral['customer_id'],
                    subcentral_id=subcentral['subcentral_id'],
                    longitude=subcentral['geo_coord_lon'],
                    latitude=subcentral["geo_coord_lat"],
                    grid_zone=subcentral["grid_zone"]
                )
                subcentral_report = runSubcentralReporter(house, cassandra_repo, house_repo, report_start, utility_config)
                
                subcentral_reports.append(subcentral_report)
                
            runGridReporter(utility, grid, aggregate_repo, report_start, subcentral_reports, flexibility_repo)
        
    dbConnection.db_shutdown()

    return json.dumps({})

if __name__ == '__main__':
    lambda_handler()


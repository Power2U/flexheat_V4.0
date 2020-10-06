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

def getActiveUtility(es):

    res = es.search(index="flexheat_customers", body={"query": {"bool": {"must": [{"term": {
                    "enable_flexibility": "true"}}], "must_not": [], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}})

    result = []

    for item in res['hits']['hits']:
        result.append(item["_source"])

    return result

def getActiveGrid(es, utility, planning_start):

    # TODO: fetch peak hours: ts_start>= planning_start and customer_id = utility
    res = es.search(index="flexheat_peak_hours", body={"query": {"bool": {"must": [{"term": {
                    "customer_id": "utility"}}], "must_not": [], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}})

    result = {} # set, without duplicate values

    for item in res['hits']['hits']:
        result.add(item["_source"]["grid_zone"]) # return grid_zone 

    return result

def getActiveSubcentrals(es, utility, grid): 

    # TODO: fetch subcentrals: customer_id = utility, grid_zone = grid, enable_fcc = true, enable_flexibility = true
    res = es.search(index="flexheat_peak_hours", body={"query": {"bool": {"must": [{"term": {
                    "customer_id": "true"}}], "must_not": [], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}})

    result = []

    for item in res['hits']['hits']:
        result.append(item["_source"]) # return subcentral record

    return result

def runSubcentralReporter(house, cassandra_repo, house_repo, report_end, utility_config):
    logger.info("start forecaster for house")
    logger.info(f"{house}")

    try:
        reporter = SubcentralReport(
            house=house,
            cassandra_repo=cassandra_repo,
            house_repo=house_repo,
            report_end=report_end,
            utility_config=utility_config
        )

        subcentral_report = reporter.report

        cassandra_repo.write_report_for_house(house, subcentral_report, utility_config)
        
        return subcentral_report

    except ValueError as ve:
        logger.error(ve)

    except Exception as ex:
        logger.error(ex)

def runGridReporter(utility, grid, aggregate_repo, flexibility_repo, report_end, subcentral_reports, flexibility_repo):
    logger.info(f"Aggregate flexibility for customer_id = {utility}, grid_zone = {grid}")

    try:
        reporter = AggregationReport(
            customer = utility,
            grid = grid,
            aggregate_repo = aggregate_repo,
            flexibility_repo = flexibility_repo,
            report_end = report_end
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
    flexibility_repo = CassandraFlexibilityRepository(session)

    his_end = datetime.utcnow()

    his_end = his_end.strftime("%Y-%m-%d %H:00:00.000Z")

    logger.info(f"Report for the period before: {his_end}")
    
    report_end = datetime.strptime(his_end, "%Y-%m-%d %H:%M:%S.%f%z")
  
    ES_URL = 'http://13.48.110.27:9200/'
    
    es = connectES(ES_URL)
    
    utilities = getActiveUtility(es) 
    
    logger.info("Active energy companies:")
    logger.info(f"customer_id = {utilities}")
        
    for utility in utilities:
        
        logger.info(f"Report for customer_id = {utility}")
        
        utility_config = aggregate_repo.get_config_by_customer(utility)
        
        grids = getActiveGrid(es, utility, planning_start) # return peak hours sorting by grid zone
        
        logger.info(f"Flexibility is needed for grid_zone = {grids}")
       
        for grid in grids:
            
            subcentral_plans = []
        
            logger.info(f"Report for grid_zone = {grid}")
            
            subcentrals = getActiveSubcentrals(es, utility, grid)# return subcentrals in a grid zone
            
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
                subcentral_report = runSubcentralReporter(house, cassandra_repo, house_repo, report_end, utility_config)
                
                subcentral_reports.append(subcentral_report)
                
            runGridReporter(utility, grid, aggregate_repo, report_end, subcentral_reports, flexibility_repo)
        
    dbConnection.db_shutdown()

    return json.dumps({})

if __name__ == '__main__':
    lambda_handler()


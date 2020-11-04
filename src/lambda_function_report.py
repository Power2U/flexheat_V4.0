import logging
import os
import json
from elasticsearch import Elasticsearch
from datetime import *
from db import *
from report._subcentral_report import SubcentralReport
from report._aggregate_report import AggregationReport
from models import *
from deploy_utils import *

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0
    Step 3.1 and 3.2 in the 3-step service delivery model
    Report the actually delivered flexibility on subcentral- and aggregation-level

'''

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
        
def lambda_handler(report_start):  

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
    logger.info(f"customer_id = {utilities}")
        
    for utility in utilities:
        
        logger.info(f"Report for customer_id = {utility}")
        
        utility_config = aggregate_repo.get_config_by_customer(utility)
        
        grids = getActiveGrid(es, utility, report_start)
                
        if bool(grids) is False:
            logger.info("No peak hours have been specified, therefore no report.")
            continue 
        
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
    
    start = datetime.utcnow() + timedelta(hours=1)
    start = start.strftime("%Y-%m-%d %H:00:00.000Z")
    report_start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S.%f%z")    
    logger.info(f"Report to start: {report_start}")    
    lambda_handler(report_start)

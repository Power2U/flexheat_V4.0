import os
import json
from elasticsearch import Elasticsearch
from db import *
from outside_temp_forecaster.predictor import OutTempPredictor
from models import *
import pytz
from datetime import *
import pandas as pd
import numpy as np
import boto3
import logging

'''
    Flexheat V4.0, same as V2.0, V3.0
    Local outside temperature forecasting
    
'''
# On prod environment there is no need to use aws profile since we are using role and policy
s3 = boto3.client('s3')

# For testing on your device you can install aws cli, and use 'aws configure' to add a profile with credentials
# session = boto3.Session(profile_name='flex-heat-prod-mfa')
# s3 = session.client('s3')

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

def getObject(fileName, bucketName='data-model-mpc-bucket'):
    try:
        with open(fileName, 'wb') as f:
            s3.download_fileobj(bucketName, fileName, f)
    except Exception as exc:
        logger.error("Error in getObject()")
        logger.error(exc)
        
    return

def getActiveSubcentrals():
    ES_URL = 'http://13.48.110.27:9200/'
    if "ES_URL" in os.environ:
        ES_URL = os.environ['ES_URL']

    es = Elasticsearch([ES_URL])

    res = es.search(index="flexheat_subcentral", body={"query": {"bool": {"must": [{"term": {
                    "enable_fcc": "true"}}], "must_not": [], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}})

    result = []

    for item in res['hits']['hits']:
        result.append(item["_source"])

    return result

def runForecaster(house, cassandra_house_repo, cassandra_weather_repo, house_model_repo, planning_start, fileName):

    logger.info("start forecaster for house")
    logger.info(f"{house}")
    
    try:
        predictor = OutTempPredictor(
            house=house,
            cassandra_house_repo=cassandra_house_repo,
            cassandra_weather_repo=cassandra_weather_repo,
            house_model_repo=house_model_repo,
            planning_start=planning_start,
            model=fileName
        )

        forecast = predictor.forecast

        cassandra_weather_repo.write_temp_deviation_for_house(house=house,
                                                              forecast=forecast,
                                                              house_repo=house_model_repo)

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
    cassandra_weather_repo = CassandraWeatherRepository(session)
    house_model_repo = RESTHouseModelRepository(session)

    planning_start = datetime.utcnow() + timedelta(hours=1)

    planning_start = planning_start.strftime("%Y-%m-%d %H:00:00.000Z")

    logger.info(f"Forecast starts: {planning_start}")

    planning_start = datetime.strptime(
        planning_start, "%Y-%m-%d %H:%M:%S.%f%z")

    subcentrals = getActiveSubcentrals()

    logger.info("Active subcentrals:")
    logger.info(f"{subcentrals}")
    
    for subcentral in subcentrals:
        house = House(
            location=subcentral["geo_city"],
            customer_id=subcentral['customer_id'],
            subcentral_id=subcentral['subcentral_id'],
            longitude=subcentral['geo_coord_lon'],
            latitude=subcentral["geo_coord_lat"]
        )

        fileName = f"outTempModel_cid{house.customer_id}_sid{house.subcentral_id}.pkl"
        logger.info("Getting SARIMAX model")
        getObject(fileName)

        runForecaster(house, cassandra_house_repo, cassandra_weather_repo,
                      house_model_repo, planning_start, fileName)

    dbConnection.db_shutdown()

    return json.dumps({})

if __name__ == "__main__":

    '''
    Note: 

    The function takes a few minutes for downloading model and less than 1 minute for next 24-hour prediction.
    Call the function when it is close to the planning_start e.g. 10-15 minutes before planning_start,
    '''

    lambda_handler()

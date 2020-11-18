import os
import json
from elasticsearch import Elasticsearch
from db import *
from outside_temp_forecaster.model_trainer import OutTempModelGenerator
from models import *
import pytz
from datetime import *
import pandas as pd
import numpy as np
import logging
import boto3
from datetime import *

'''
    Flexheat V4.0, same as flexheat V2.0 and V3.0
    Generating local outside temperature forecaster

'''
# On prod environment there is no need to use aws profile since we are using role and policy
s3 = boto3.client('s3')

# For testing on your device you can install aws cli, and use 'aws configure' to add a profile with credentials
# session = boto3.Session(profile_name='flex-heat-prod-mfa')
# s3 = session.client('s3')

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def createObject(fileName, bucketName='data-model-mpc-bucket'):
    try:
        with open(fileName, "rb") as f:
            s3.upload_fileobj(f, bucketName, fileName)
    except Exception as exc:
        logger.error("Error in createObject()")
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


def runModelGenerator(house, cassandra_house_repo, cassandra_weather_repo, house_model_repo, his_end):
    logger.info("start model generator for house")
    logger.info(f"{house}")

    try:
        model_generator = OutTempModelGenerator(
            house=house,
            cassandra_house_repo=cassandra_house_repo,
            cassandra_weather_repo=cassandra_weather_repo,
            house_model_repo=house_model_repo,
            his_end=his_end
        )

        model = model_generator.model

        logger.info("Saving SARIMAX model")
        fileName = f"outTempModel_cid{house.customer_id}_sid{house.subcentral_id}.pkl"
        model.save(fileName)
        logger.info("Model file is saved!")                
        logger.info("Uploading SARIMAX model")        

        createObject(fileName)

    except ValueError as ve:
        logger.error(ve)
        print(ve)

    except Exception as ex:
        logger.error(ex)
        print(ex)


def lambda_handler():

    DB_URL = '13.48.110.27'
    if "DB_URL" in os.environ:
        DB_URL = os.environ['DB_URL']

    dbConnection = DBConnection(DB_URL)
    session = dbConnection.session

    cassandra_house_repo = CassandraHouseRepository(session)
    cassandra_weather_repo = CassandraWeatherRepository(session)
    house_model_repo = RESTHouseModelRepository(session)

    his_end = datetime.utcnow() + timedelta(hours=1)

    his_end = his_end.strftime("%Y-%m-%d %H:00:00.000Z")

    logger.info(f"Training set ends: {his_end}")
    
    his_end = datetime.strptime(
        his_end, "%Y-%m-%d %H:%M:%S.%f%z")

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

        runModelGenerator(house, cassandra_house_repo,
                          cassandra_weather_repo, house_model_repo, his_end)

    dbConnection.db_shutdown()

    return json.dumps({})


if __name__ == "__main__":

    '''
    Note: 

    The function takes 2-3 minutes to train the model with current configuration and a few minutes to upload.
    Call the function once per day to generate a new model for each subcentral.    

    '''

    lambda_handler()

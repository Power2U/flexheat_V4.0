import logging
import os
import json
from elasticsearch import Elasticsearch
from datetime import *
from db import *
from dynamic_model.model_trainer import DynamicModelGenerator
from models import *

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0, same as Flexheat V3.0
    Generating dynamic model coefficients

'''

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

def runModelGenerator(house, cassandra_house_repo, cassandra_weather_repo, cassandra_repo, house_model_repo, his_end):

    logger.info("start model generator for house")
    logger.info(f"{house}")
    
    try:
        model_generator = DynamicModelGenerator(
            house=house,
            cassandra_house_repo=cassandra_house_repo,
            cassandra_weather_repo=cassandra_weather_repo,
            house_model_repo=house_model_repo,
            cassandra_repo=cassandra_repo,
            his_end=his_end
        )
                            
        # New model is saved only when the in-sample mean square error is within a threshold.
        # 0.5 is used as current threshold
        if model_generator.mse <= 0.5:           
            cassandra_house_repo.write_model_for_house(house, model_generator.model_coef, model_generator.model_intercept)     
        else:
            logger.info("MSE of model is larger than threshold and not saved")         
            
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
    cassandra_repo = CassandraRepository(session) 
    house_model_repo = RESTHouseModelRepository(session)
    
    his_end = datetime.utcnow()

    his_end = his_end.strftime("%Y-%m-%d %H:00:00.000Z")

    logger.info(f"Training set ends: {his_end}")

    his_end = datetime.strptime(
        his_end, "%Y-%m-%d %H:%M:%S.%f%z")

    subcentrals = getActiveSubcentrals()

    print('active subcentrals')
    print(subcentrals)

    for subcentral in subcentrals:
        house = House(
            location=subcentral["geo_city"],
            customer_id=subcentral['customer_id'],
            subcentral_id=subcentral['subcentral_id'],
            longitude=subcentral['geo_coord_lon'],
            latitude=subcentral["geo_coord_lat"]
        )
        runModelGenerator(house, cassandra_house_repo, cassandra_weather_repo, cassandra_repo, house_model_repo, his_end)

    dbConnection.db_shutdown()

    return json.dumps({})

if __name__ == '__main__':

    '''
    Note: 

    The function takes 2-3 minutes to train the model.
    Call the function once per day to generate a new model for each subcentral.    

    '''
    
    lambda_handler()


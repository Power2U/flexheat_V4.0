import logging
from lambda_function_execution import *

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0
    Step 2.1 in the 3-step service delivery model
    Distribute dispatch orders for subcentrals
    
    Test the execution function lambda_handler (in lambda_function_execution.py) with a specific report_start

'''

if __name__ == '__main__':

    planning_start = "2020-10-01 00:00:00.000Z"
    planning_start = datetime.strptime(planning_start, "%Y-%m-%d %H:%M:%S.%f%z")    
    logger.info(f"Planning to start: {planning_start}")    
    lambda_handler(planning_start)

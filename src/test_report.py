import logging
from lambda_function_report import *

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

'''
    Flexheat V4.0
    Step 3.1 and 3.2 in the 3-step service delivery model
    Report the actually delivered flexibility on subcentral- and aggregation-level
    
    Test the report function lambda_handler (in lambda_function_report.py) with a specific report_start

'''

if __name__ == '__main__':

    report_start = "2020-10-01 00:00:00.000Z"
    report_start = datetime.strptime(report_start, "%Y-%m-%d %H:%M:%S.%f%z")    
    logger.info(f"Report to start: {report_start}")    
    lambda_handler(report_start)

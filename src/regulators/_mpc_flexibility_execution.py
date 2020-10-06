from forecasters._forecast_flexibility import HouseDataForecaster
import pandas as pd
import pytz
import logging

logger = logging.getLogger("__main__")

class MPCController_Execution:
    def __init__(self, forecaster: HouseDataForecaster):
        
        self._forecaster = forecaster

    def control(self) -> pd.DataFrame:
        logger.info(f"Fetching planning data for {self._forecaster._house.customer_id, self._forecaster._house.subcentral_id}")
                
        output = self._forecaster.optimization_model_execution().solve(self._forecaster.forecast_data, 
                                                             self._forecaster.initial_data,
                                                             self._forecaster.diff_data,                                                                                                    
                                                             self._forecaster.heatcurve)

        return output
    



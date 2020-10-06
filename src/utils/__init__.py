import models
from pvlib.location import Location as PVLocation
from pvlib.forecast import ForecastModel
import logging

logger = logging.getLogger(__name__)

def estimate_solar(house: models.House, cloud_cover):
    irradiance_clearsky = calculate_irradiance(
        cloud_cover.index,
        latitude=house.latitude,
        longitude=house.longitude,
    )
       
    data = irradiance_clearsky.join(cloud_cover.rename('total_cloud_cover'))
    data['cloud_cover_percent'] = data['total_cloud_cover'] * 100 / 8 # convert from integer 0-8 to percentage 0-100 cloud coverage
    
    model = ForecastModel(model_type='Forecast', model_name='SMHI Forecast', set_type='foo')
    model.set_location(tz = cloud_cover.index.tzinfo,
#                       time = cloud_cover.index, # API has changed, argument time is not valid 
                       latitude=house.latitude,
                       longitude=house.longitude)
    irradance_cloud = model.cloud_cover_to_irradiance(data['cloud_cover_percent'], how='clearsky_scaling') # alternative: how='liujordan'
    return irradance_cloud['ghi']
    
def calculate_irradiance(index, latitude, longitude):
    pv_location = PVLocation(
        latitude=latitude,
        longitude=longitude
    )
    return pv_location.get_clearsky(index)

def estimate_baseline_power(heatcurve: models.HeatCurve, out_temp):
        
    if out_temp > max(heatcurve.out_temp):
        
        logger.warning("Outside temperature is higher than the upper limit on heat curve")   
        
        baseline_power = 0 # Heating is assumed turned off
        
        return baseline_power  
        
    elif out_temp <= min(heatcurve.out_temp):  
        
        logger.warning("Outside temperature is lower than the lower limit on heat curve")   
        
        baseline_power = max(heatcurve.power) # Max heating rate
        
        return baseline_power    

    else:
            for i in range(heatcurve.break_point - 1):
        
                if out_temp > heatcurve.out_temp[i] and out_temp <= heatcurve.out_temp[i+1]:
                    
                    baseline_power = heatcurve.power[i] + (out_temp - heatcurve.out_temp[i]) * \
                    (heatcurve.power[i+1] - heatcurve.power[i]) / (heatcurve.out_temp[i+1] - heatcurve.out_temp[i]) 
                    
                    return baseline_power
                
                    break       
                
                else:
                    continue

def estimate_reference_inflow_temp(heatcurve: models.HeatCurve, out_temp):
        
    if out_temp > max(heatcurve.out_temp):
        
        logger.warning("Outside temperature is higher than the upper limit on heat curve")   
        
        reference_inflow_temp = min(heatcurve.inflow_temp) # Heating is assumed turned off
        
        return reference_inflow_temp  
        
    elif out_temp <= min(heatcurve.out_temp):  
        
        logger.warning("Outside temperature is lower than the lower limit on heat curve")   
        
        baseline_power = max(heatcurve.inflow_temp) # Max heating rate
        
        return baseline_power    

    else:
            for i in range(heatcurve.break_point - 1):
        
                if out_temp > heatcurve.out_temp[i] and out_temp <= heatcurve.out_temp[i+1]:
                    
                    reference_inflow_temp = heatcurve.inflow_temp[i] + (out_temp - heatcurve.out_temp[i]) * \
                    (heatcurve.inflow_temp[i+1] - heatcurve.inflow_temp[i]) / (heatcurve.out_temp[i+1] - heatcurve.out_temp[i]) 
                    
                    return reference_inflow_temp
                
                    break       
                
                else:
                    continue
        
def estimate_inflow_temp_offset(heatcurve: models.HeatCurve, out_temp, power_offset):
    
    baseline_power = estimate_baseline_power(heatcurve, out_temp)
    reference_inflow_temp = estimate_reference_inflow_temp(heatcurve, out_temp)
    new_power = baseline_power + power_offset

    if new_power > max(heatcurve.power):
         
        logger.warning("new power is higher than the upper limit on heat curve")   
                   
        inflow_temp_offset = max(heatcurve.inflow_temp) - reference_inflow_temp
        
        return inflow_temp_offset  
         
    elif new_power <= min(heatcurve.power):  
         
        logger.warning("new power is lower than the lower limit on heat curve")   
         
        inflow_temp_offset = min(heatcurve.inflow_temp) - reference_inflow_temp
             
        return inflow_temp_offset
    
    else:
            for i in range(heatcurve.break_point - 1):
        
                if new_power <= heatcurve.power[i] and new_power > heatcurve.power[i+1]:
                                     
                    # Simplification: linear relation is used according to Stallmastaregatan case
                    new_inflow_temp = heatcurve.inflow_temp[i] + (new_power - heatcurve.power[i]) * \
                    (heatcurve.inflow_temp[i+1] - heatcurve.inflow_temp[i]) / (heatcurve.power[i+1] - heatcurve.power[i]) 
                    
                    inflow_temp_offset = new_inflow_temp - reference_inflow_temp
                    
                    return inflow_temp_offset, new_inflow_temp
                                    
                    break       
                
                else:
                    continue   


def estimate_initial_power(heatcurve: models.HeatCurve, inflow_temp):
    
    if inflow_temp >= max(heatcurve.inflow_temp):
        
        logger.warning("Initial inflow temperature is higher than the upper limit on heat curve")   
        
        power = max(heatcurve.power) 
        
        return power  
        
    elif inflow_temp < min(heatcurve.inflow_temp):  
        
        logger.warning("Initial inflow temperature is lower than the lower limit on heat curve")   
        
        power = heatcurve.power[heatcurve.break_point-1] + (inflow_temp - heatcurve.inflow_temp[heatcurve.break_point-1]) * \
                    (heatcurve.power[heatcurve.break_point-1] - heatcurve.power[heatcurve.break_point-2]) \
                    / (heatcurve.inflow_temp[heatcurve.break_point-1] - heatcurve.inflow_temp[heatcurve.break_point-2])         
        
        return power
        
    else:
            for i in range(heatcurve.break_point - 1):
        
                if inflow_temp < heatcurve.inflow_temp[i] and inflow_temp >= heatcurve.inflow_temp[i+1]:
                    
                    power = heatcurve.power[i] + (inflow_temp - heatcurve.inflow_temp[i]) * \
                    (heatcurve.power[i+1] - heatcurve.power[i]) / (heatcurve.inflow_temp[i+1] - heatcurve.inflow_temp[i]) 
                    
                    return power
                                    
                    break       
                
                else:
                    continue   
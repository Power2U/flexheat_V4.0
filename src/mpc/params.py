from dataclasses import dataclass, field
import typing
from typing import List

# Default values are set for parameters

@dataclass
class PhysicalHouseParameters:    
    capacitance: float = 1e6 # [kJ/K]
    heat_loss_coeff: float = 6
    solar_area: float = 100
    control_valve_alpha: float = 1
    
    def __init__(self, parameters):
        if not (parameters["physical_capacitance"] is None):
            self.capacitance = parameters["physical_capacitance"]
            
        if not (parameters["physical_solar_area"] is None):            
            self.solar_area = parameters["physical_solar_area"]
        
        if not (parameters["physical_control_valve_alpha"] is None):            
            self.control_valve_alpha = parameters["physical_control_valve_alpha"]
            
        if not (parameters["physical_heat_loss_coeff"] is None):            
            self.heat_loss_coeff = parameters["physical_heat_loss_coeff"]

@dataclass
class HouseSimulationParameters:
    thermal_coeff: float = 0.4
    solar_coeff: float = 0.1   
    heating_coeff: float = 0.4
    a: float = 1
    b: float = 0

    def __init__(self, parameters):
        if not (parameters["simulation_thermal_coeff"] is None):
            self.thermal_coeff = parameters["simulation_thermal_coeff"]
            
        if not (parameters["simulation_solar_coeff"] is None):            
            self.solar_coeff = parameters["simulation_solar_coeff"]
        
        if not (parameters["simulation_heating_coeff"] is None):            
            self.heating_coeff = parameters["simulation_heating_coeff"]
            
        if not (parameters["simulation_a"] is None):            
            self.a = parameters["simulation_a"]    
            
        if not (parameters["simulation_b"] is None):            
            self.b = parameters["simulation_b"]  
            
@dataclass
class MPCConfiguration:

    optimization_horizon: int = 48 # optimization_horizon: number of time steps
    timestep: int = 1800 # unit: second
    rate_limit_lower: float = 70
    rate_limit_upper: float = 600
    below_error_priority: float = 5000
    energy_price: float = 0.403
    energy_price_priority: float = 1
    max_power_offset: float = 106
    setpoint: float = 20
    max_ramp: float = 106
    # Add for flexibility service
    above_error_priority: float = 1
    hysteresis_above: float = 0.5
    hysteresis_below: float = 0.5
    flexibility_price: float = 1.000
    flexibility_price_priority: float = 1
    rebound_limit: float = 0.5
    
    def __init__(self, parameters):
        if not (parameters["mpc_optimization_horizon"] is None):
            self.optimization_horizon = parameters["mpc_optimization_horizon"]
            
        if not (parameters["mpc_timestep"] is None):
            self.timestep = parameters["mpc_timestep"]
            
        if not (parameters["mpc_rate_limit"] is None):
            self.rate_limit_lower = parameters["mpc_rate_limit_lower"]

        if not (parameters["mpc_rate_limit"] is None):
            self.rate_limit_upper = parameters["mpc_rate_limit"]

        if not (parameters["mpc_below_error_priority"] is None):
            self.below_error_priority = parameters["mpc_below_error_priority"]

        if not (parameters["mpc_energy_price"] is None):
            self.energy_price = parameters["mpc_energy_price"]
            
        if not (parameters["mpc_energy_price_priority"] is None):
            self.energy_price_priority = parameters["mpc_energy_price_priority"]
            
        if not (parameters["mpc_max_power_offset"] is None):
            self.max_power_offset = parameters["mpc_max_power_offset"]

        if not (parameters["mpc_setpoint"] is None):
            self.setpoint = parameters["mpc_setpoint"]            
            
        if not (parameters["mpc_max_ramp"] is None):
            self.max_ramp = parameters["mpc_max_ramp"]
        
        # Add for flexibility service
        if not (parameters["flex_above_error_priority"] is None):
            self.above_error_priority = parameters["flex_above_error_priority"]
        if not (parameters["flex_hysteresis_above"] is None):
            self.hysteresis_above = parameters["flex_hysteresis_above"]
        if not (parameters["flex_hysteresis_below"] is None):
            self.hysteresis_below = parameters["flex_hysteresis_below"]
        if not (parameters["flex_flexibility_price"] is None):
            self.flexibility_price = parameters["flex_flexibility_price"]
        if not (parameters["flex_flexibility_price_priority"] is None):
            self.flexibility_price = parameters["flex_flexibility_price_priority"]
        if not (parameters["flex_rebound_limit"] is None):
            self.rebound_limit = parameters["flex_rebound_limit"]            
     
@dataclass
class DynamicConfiguration:

    in_temp_diff_lag: List[int] = field(default_factory=list)
    out_temp_diff_lag: List[int] = field(default_factory=list)
    solar_diff_lag: List[int] = field(default_factory=list)
    training_length: int = 240

    def __init__(self, parameters):
            
        if not (parameters["dynamic_in_temp_diff_lag"] is None):
            self.in_temp_diff_lag = parameters["dynamic_in_temp_diff_lag"]  
        else:
            self.in_temp_diff_lag = [0, 24] 
                 
        if not (parameters["dynamic_out_temp_diff_lag"] is None):
            self.out_temp_diff_lag = parameters["dynamic_out_temp_diff_lag"]  
        else:
            self.out_temp_diff_lag = [0, 24]               

        if not (parameters["dynamic_solar_diff_lag"] is None):
            self.solar_diff_lag = parameters["dynamic_solar_diff_lag"]  
        else:
            self.solar_diff_lag = [0, 24]  

        if not (parameters["dynamic_train_length"] is None):# training length for dynamic model
            self.training_length = parameters["dynamic_train_length"]                

@dataclass
class DynamicModel:
   
    intercept: float = 0    

    variable_coef: List[float] = field(default_factory=list)
    
    def __init__(self, parameters):

        if not (parameters["intercept"] is None):
            self.intercept = parameters["intercept"]
            
        if not (parameters["variable_coef"] is None):
            self.variable_coef = parameters["variable_coef"]  
        else:
            self.variable_coef = [7.2372785, 2.614665, -0.9645129, 198.13611, 74.72733, 0.11609552, -0.0048479806, -1.4043026, -1.4628465, -0.14104348, -0.10889917]

@dataclass
class SARIMAXConfiguration:
   
    sarimax_train_length: int = 384    

    sarimax_pdq_PDQ_s: List[int] = field(default_factory=list)
    
    def __init__(self, parameters):

        if not (parameters["sarimax_train_length"] is None):
            self.sarimax_train_length = parameters["sarimax_train_length"]
            
        if not (parameters["sarimax_param"] is None):
            self.sarimax_pdq_PDQ_s = parameters["sarimax_param"]  
        else:
            self.sarimax_pdq_PDQ_s = [2, 0, 0, 0, 1, 2, 48]

@dataclass
class FlexibilityConfiguration: # For energy company

    planning_horizon: int = 24 # planning_horizon: number of time steps
    timestep: int = 3600 # unit: second
   
    def __init__(self, parameters):
        if not (parameters["planning_horizon"] is None):
            self.planning_horizon = parameters["planning_horizon"]
            
        if not (parameters["timestep"] is None):
            self.timestep = parameters["timestep"]
            


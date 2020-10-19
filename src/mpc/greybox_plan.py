import pandas as pd
import numpy as np
import cvxpy as cp
from mpc.params import *
from attrdict import AttrDict
import logging
from utils import estimate_inflow_temp_offset

logger = logging.getLogger("__main__")

class Variables(AttrDict):
    pass

class Parameters(AttrDict):
    pass

class PnPkModel_Plan(object):

    def __init__(
            self,
            physical: PhysicalHouseParameters,
            mpc_config: MPCConfiguration,
            dynamic_config: DynamicConfiguration,
            dynamic_model: DynamicModel):

        self._physical = physical
        self._config = mpc_config
        
        # Add for auto dynamic model
        self._dynamic = dynamic_config 
        self._model = dynamic_model   
            
        self._horizon = self._config.optimization_horizon
        self._problem = None
        self._timestep = self._config.timestep
        
        # Add for auto dynamic model
        self._max_lag = max(max(self._dynamic.in_temp_diff_lag), max(self._dynamic.out_temp_diff_lag), max(self._dynamic.solar_diff_lag))        
        
        self._init_variables()
        self._init_parameters()

    def _init_variables(self):
        
        logger.info("Initializing optimization variables")
        self._variables = Variables()
                
        '''
        Index for power: [0, 1, ..., horizon - 1], index for temperature: [0, 1, 2, ..., horizon]
        '''        
        self._variables.power = cp.Variable(shape = self._horizon, name='Power', nonneg=True)
        self._variables.temperature = cp.Variable(shape = self._horizon + 1, name='Temperature')
        self._variables.below_error = cp.Variable(shape = self._horizon + 1, name='Temp below error', nonneg=True)
        
        # Add for auto dynamic model
        self._variables.in_temp_diff = cp.Variable(shape = self._max_lag + self._horizon, name = '1st order differencing of indoor temperature')
        
        # Add for flexibility service
        self._variables.above_error = cp.Variable(shape = self._horizon + 1, name = 'Temp above error', nonneg=True)
    
    def _init_parameters(self):
        
        logger.info("Initializing optimization parameters")
        self._parameters = Parameters()
        self._parameters.outdoor_temp = cp.Parameter(shape = self._horizon, name='Outdoor temperature')
        self._parameters.solar = cp.Parameter(shape = self._horizon, name='Solar power')
        self._parameters.initial_temperature = cp.Parameter(name='Initial temperatures')
        self._parameters.initial_power = cp.Parameter(name='Initial power')
        self._parameters.energy_price = cp.Parameter(name='Heating energy price')
        self._parameters.baseline_power = cp.Parameter(shape = self._horizon, name='Reference power')
        self._parameters.max_power_offset = cp.Parameter(name='Max power offset')          
        self._parameters.max_ramp = cp.Parameter(name='Max power ramping')
        self._parameters.setpoint = cp.Parameter(name='Temperature target')
        self._parameters.below_error_priority = cp.Parameter(name='Cost amplification below error')
        self._parameters.energy_price_priority = cp.Parameter(name='Cost amplification of energy price')
        self._parameters.rate_limit_upper = cp.Parameter(name='Upper rate limit')
        self._parameters.rate_limit_lower = cp.Parameter(name='Lower rate limit')
        
        # Add for auto dynamic model
        self._parameters.out_temp_diff = cp.Parameter(shape = self._max_lag + self._horizon, name = '1st order differencing of outside temperature')
        self._parameters.solar_diff = cp.Parameter(shape = self._max_lag + self._horizon, name = '1st order differencing of solar power')
        self._parameters.in_temp_diff_known = cp.Parameter(shape = self._max_lag + self._horizon, name = '1st order differencing of indoor temperature')

        # Add for flexibility service
        self._parameters.hysteresis_above = cp.Parameter(name = 'Hysteresis for indoor temperature above the set point')
        self._parameters.hysteresis_below = cp.Parameter(name = 'Hysteresis for indoor temperature below the set point')
        self._parameters.peak_hour = cp.Parameter(shape = self._horizon, name = 'Peak hour') # 1: peak hour, 0: off-peak hour
        self._parameters.flexibility_price_priority = cp.Parameter(name = 'Cost amplification of flexibility price')
        self._parameters.flexibility_price = cp.Parameter(name = 'Flexibility price')
        self._parameters.above_error_priority = cp.Parameter(name = 'Cost amplification of above error')
        self._parameters.rebound_limit = cp.Parameter(shape = self._horizon, name = 'Rebound limit')
        
    @property
    def parameters(self):
        return self._parameters

    @property
    def variables(self):
        return self._variables
    
    # Dynamic model to estimate indoor temperature
    # Linear constraint of the optimization
    @property
    def dynamics(self) -> typing.List[cp.constraints.zero.Equality]:

        logger.info(f'Constructing dynamics constraints, horizon {self._config.optimization_horizon}')
        
        v = self._variables
        p = self._parameters
        dynamic = self._dynamic
        model = self._model
        house = self._physical
#         model.variable_coef = [0.0, 0.9452124, 0.33690313, 5.348071E-4, -0.024608968, -0.16360809, 0.043476336, 0.017029949, -0.008980742, 0.1853629, -0.6580194]
#         model.intercept = 1.048 

        # Define dynamic model
        dynamic_model = []
        
        for t in range(1, self._horizon + 1):
            
            # Define model variables
            '''
            Sequence matters!!!
            Sequence keeps consistent with ../dynamic_model/model_trainer.py: DynamicModelGenerator.training_data()            
            Alternative: Multiply indoor-outdoor temperature difference with house.heat_loss_coeff, reflecting physical relationships
            '''                
#             model_variables =[v.temperature[t - 1], 
#                                       p.solar[t-1],
#                                       v.power[t-1],
#                                       (v.temperature[t-1] - p.outdoor_temp[t-1]) * house.heat_loss_coeff]
            model_variables =[v.temperature[t - 1], 
                                      p.solar[t-1],
                                      v.power[t-1],
                                      (v.temperature[t-1] - p.outdoor_temp[t-1])]            
     
            for i in dynamic.in_temp_diff_lag:
                model_variables.append(v.in_temp_diff[self._max_lag + t - 1 - i])
            
            for i in dynamic.out_temp_diff_lag:
                model_variables.append(p.out_temp_diff[self._max_lag + t - 1 - i])
                                      
            for i in dynamic.solar_diff_lag:
                model_variables.append(p.solar_diff[self._max_lag + t - 1 - i])
            
            # Define dynamic model equation              
            if len(model.variable_coef) != len(model_variables) + 1:
                logger.error('Length of dynamic model variable_coef <> length of model variables.')
                
            eq = 1 * model.variable_coef[0]
            
            for i in range(len(model_variables)):
                
#                 '''
#                 Multiply each model variables with self._timestep / house.capacitance, reflecting physical relationships
#                 '''                
#                eq += model_variables[i] * model.variable_coef[i+1] * self._timestep / house.capacitance
                eq += model_variables[i] * model.variable_coef[i+1]
                
            eq += model.intercept
            
            dynamic_model.append(eq)
        
        return [
            v.temperature[t] == dynamic_model[t-1]  for t in range(1, self._horizon + 1)
        ]  
        
    # Other constraints of the optimization        
    def to_problem(self):
        
        logger.info('Constructing optimization problem')

        v = self._variables
        p = self._parameters

        logger.info('Constructing optimization constraints')

        initial = [
            v.temperature[0] == p.initial_temperature
        ]
           
        # Add for auto dynamic model: 1st order differencing of indoor temperature 
        temperature_diff = [
            v.in_temp_diff[i] == p.in_temp_diff_known[i] for i in range(0, self._max_lag + 1)
        ] + [            
            v.in_temp_diff[i] == v.temperature[i - self._max_lag] - v.temperature[i - self._max_lag -1] 
            for i in range(self._max_lag + 1, self._max_lag + self._horizon)
        ]
        
        rate_limit = [
            v.power[t] <= p.rate_limit_upper for t in range(self._horizon)
        ] + [
            v.power[t] >= p.rate_limit_lower for t in range(self._horizon)
        ]
        
#         errors = [
#             v.below_error[t] >= p.setpoint - p.hysteresis_below - v.temperature[t] for t in range(self._horizon + 1)
#         ]
        errors = [
            v.below_error[t] >= p.setpoint - p.hysteresis_below - v.temperature[t] for t in range(self._horizon + 1)
        ] + [
            v.above_error[t] >= v.temperature[t] - p.setpoint - p.hysteresis_above for t in range(self._horizon + 1)
        ]
                
        reference = [
            v.power[t] <= p.baseline_power[t] + self.parameters.max_power_offset for t in range(self._horizon)
        ] + [
            v.power[t] >= p.baseline_power[t] - self.parameters.max_power_offset for t in range(self._horizon)
        ]

        smoothness = [
            v.power[t] - v.power[t-1] <= self.parameters.max_ramp for t in range(1, self._horizon)
        ] + [
            v.power[t-1] - v.power[t] <= self.parameters.max_ramp for t in range(1, self._horizon)
        ] + [
            v.power[0] - p.initial_power <= self.parameters.max_ramp 
        ] + [
            p.initial_power - v.power[0] <= self.parameters.max_ramp 
        ]

        # Add for flexibility service
        rebound = [
            v.power[t] <= p.baseline_power[t] * (1 + p.rebound_limit) for t in range(1, self._horizon)
            ]        
        
        constraints = self.dynamics + initial + temperature_diff + rate_limit + errors + reference + smoothness + rebound
        
        return cp.Problem(
            objective=self.objective,
            constraints=constraints
        )

    # Objective function of the optimization
    @property
    def objective(self):

        logger.info('Constructing objective function')
        
        '''
        Cost function:
        
        Imaginary penalty for temperature deviation when below the lowest requirement
        + Imaginary penalty for temperature deviation when above the highest requirement
        + Energy cost
        + Income from flexibility service: 
            Assuming the income is only obtained when providing power reduction in peak hours, 
            whereas a power increase in peak hours may lead to an extra cost.                    
            Revision would be needed if other business models are adopted.

        The priorities are used to adjust the significance of each component
        
        '''

        v = self._variables
        p = self._parameters
        
        return cp.Minimize(
                p.below_error_priority * sum(v.below_error)
                + p.above_error_priority * sum(v.above_error)
                + p.energy_price_priority * p.energy_price * sum(v.power) * self._timestep / 3600
                - p.flexibility_price_priority * p.flexibility_price * sum((baseline - power) * peak \
                   for baseline, power, peak in zip(p.baseline_power, v.power, p.peak_hour)) \
                   * self._timestep / 3600
        )
        
    def solve(self, forecast_data, initial_data, diff_data, heatcurve):

        logger.info("Setting values for optimization parameters")
        self._parameters.outdoor_temp.value = forecast_data.out_temp.values
        self._parameters.solar.value = forecast_data.predict_solar.values
        self._parameters.initial_temperature.value = initial_data.average_indoor_temperature#
        self._parameters.initial_power.value = initial_data.heat_power
        self._parameters.energy_price.value = self._config.energy_price
        self._parameters.baseline_power.value = forecast_data.baseline_power.values
        self._parameters.max_power_offset.value = 200#self._config.max_power_offset  
        self._parameters.max_ramp.value = 300#self._config.max_ramp
        self._parameters.setpoint.value = 23.5#self._config.setpoint
        self._parameters.below_error_priority.value = self._config.below_error_priority
        self._parameters.energy_price_priority.value = self._config.energy_price_priority
        self._parameters.rate_limit_lower = self._config.rate_limit_lower
        self._parameters.rate_limit_upper = self._config.rate_limit_upper
        
        # Add for dynamic model
        self._parameters.out_temp_diff = diff_data.out_temp_diff.values
        self._parameters.solar_diff = diff_data.solar_diff.values
        self._parameters.in_temp_diff_known = diff_data.in_temp_diff.values

        # Add for flexibility service
        self._parameters.hysteresis_above = self._config.hysteresis_above
        self._parameters.hysteresis_below = self._config.hysteresis_below
        self._parameters.peak_hour = forecast_data.peak_hour.values
        self._parameters.flexibility_price_priority = self._config.flexibility_price_priority
        self._parameters.flexibility_price = self._config.flexibility_price_priority
        self._parameters.above_error_priority = self._config.above_error_priority
        self._parameters.rebound_limit = self._config.rebound_limit
                        
        problem = self.to_problem()

        logger.info('Call CVXOPT for solution...')

        problem.solve(solver=cp.CVXOPT)
        
        output = pd.DataFrame(index=forecast_data.index)
        
        if problem.status != cp.OPTIMAL:
            raise ValueError("Solver did not find solutions!")
                
        else:
            
            logger.info('Solution found!')
    
            output['out_temp_forecast'] = forecast_data.forecast_outside_temp.values
            output['out_temp_with_deviation'] = forecast_data.out_temp.values
            output['power'] = self.variables.power.value
            output['indoor_temperature'] = self.variables.temperature[1:].value
            output['baseline_power'] = forecast_data.baseline_power.values
            output['power_offset'] = output['power'] - output['baseline_power']
            output['below_error'] = self.variables.below_error[1:].value
            output['solar'] = forecast_data.predict_solar.values
            
            for index, row in output.iterrows():
                
                inflow_temp_offset, new_inflow_temp = estimate_inflow_temp_offset(heatcurve, row['out_temp_with_deviation'], row['power_offset'])
                output.at[index, 'inflow_temp_offset'] = inflow_temp_offset
                output.at[index, 'new_inflow_temp'] = new_inflow_temp
                
#             print(output['indoor_temperature'])
#             print(output['power'])
#             print(output['baseline_power'])
#             print(output['power_offset'])
#             print(output['out_temp_forecast'])
#             print(output['out_temp_with_deviation'])
#             print(output['inflow_temp_offset'])
#             print(output['new_inflow_temp'])
#             print(output['below_error'])
               
            return output
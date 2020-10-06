import abc


class HouseForecaster:

    @abc.abstractmethod
    def get_time_range(self):
        pass

    @abc.abstractmethod
    def source_data(self):
        pass

    @abc.abstractmethod
    def baseline_power(self):
        pass

    @abc.abstractmethod
    def solar(self):
        pass

    @abc.abstractmethod
    def initial_status(self):
        pass

    @abc.abstractmethod
    def optimization_model(self):
        pass    

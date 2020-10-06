from dataclasses import dataclass
from dataclasses_json import dataclass_json
import typing
import pytz

   
@dataclass_json
@dataclass(eq=True, frozen=True)

class Flexibility: 
    
    customer_id: int
    grid_zone: int
    tstamp_record: pytz.datetime.datetime
    ts_start: pytz.datetime.datetime
    ts_end: pytz.datetime.datetime
    power_offset: float


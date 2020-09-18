CREATE TABLE flexheat.flexibility_config (
	customer_id int,
	tstamp_record timestamp,
	planning_horizon int, 
	active boolean,
	time_resolution int, 
	PRIMARY KEY (customer_id, tstamp_record)
) WITH CLUSTERING ORDER BY (tstamp_record DESC);

CREATE TABLE flexheat.peak_hours (
	customer_id int,
	grid_zone int,
	ts_start timestamp,
	ts_end timestamp,	
	tstamp_record timestamp,
	PRIMARY KEY (customer_id, grid_zone, tstamp_record, ts_start)
) WITH CLUSTERING ORDER BY (grid_zone ASC, tstamp_record DESC, ts_start ASC);

CREATE TABLE flexheat.subcentral_flexibility_plan (
	customer_id int,
	subcentral_id int,
	grid_zone int,
	ts_start timestamp,
	ts_end timestamp,
	outside_temperature float,
	heating_baseline float,
	actual_power float,	
	power_offset float,
	average_indoor_temperature float,
	tstamp_record timestamp,
	PRIMARY KEY (customer_id, subcentral_id, grid_zone, tstamp_record, ts_start)
) WITH CLUSTERING ORDER BY (subcentral_id ASC, grid_zone ASC, tstamp_record DESC, ts_start ASC);

CREATE TABLE flexheat.aggregate_flexibility_plan (
	customer_id int,
	grid_zone int,
	ts_start timestamp,
	ts_end timestamp,	
	power_offset float,
	tstamp_record timestamp,
	PRIMARY KEY (customer_id, grid_zone, tstamp_record, ts_start)
) WITH CLUSTERING ORDER BY (grid_zone ASC, tstamp_record DESC, ts_start ASC);


CREATE TABLE flexheat.aggregate_flexibility_dispatch (
	customer_id int,
	grid_zone int,
	ts_start timestamp,
	ts_end timestamp,	
	power_offset float,	
	tstamp_record timestamp,
	PRIMARY KEY (customer_id, grid_zone, tstamp_record, ts_start)
) WITH CLUSTERING ORDER BY (grid_zone ASC, tstamp_record DESC, ts_start ASC);

CREATE TABLE flexheat.subcentral_flexibility_dispatch (
	customer_id int,
	subcentral_id int,
	grid_zone int,
	ts_start timestamp,
	ts_end timestamp,	
	power_offset float,	
	tstamp_record timestamp,
	PRIMARY KEY (customer_id, subcentral_id, grid_zone, tstamp_record, ts_start)
) WITH CLUSTERING ORDER BY (subcentral_id ASC, grid_zone ASC, tstamp_record DESC, ts_start ASC);

CREATE TABLE flexheat.aggregate_flexibility_delivered (
	customer_id int,
	grid_zone int,
	ts_start timestamp,
	ts_end timestamp,	
	power_offset float,
	tstamp_record timestamp,
	PRIMARY KEY (customer_id, grid_zone, tstamp_record, ts_start)
) WITH CLUSTERING ORDER BY (grid_zone ASC, tstamp_record DESC, ts_start ASC);

CREATE TABLE flexheat.subcentral_flexibility_delivered (
	customer_id int,
	subcentral_id int,
	grid_zone int,	
	ts_start timestamp,
	ts_end timestamp,
	outside_temperature float,
	heating_baseline float,
	actual_power float,	
	power_offset float,
	average_indoor_temperature float,
	tstamp_record timestamp,
	PRIMARY KEY (customer_id, subcentral_id, grid_zone, tstamp_record, ts_start)
) WITH CLUSTERING ORDER BY (subcentral_id ASC, grid_zone ASC, tstamp_record DESC, ts_start ASC);

-- ECE656
CREATE DATABASE Project;
use Project;
drop table if exists Airport_Weather;
drop table if exists AirportCoordinates;
drop table if exists Employees;
drop table if exists CARRIER_SUMMARY_AIRPORT_ACTIVITY;
drop table if exists OntimeReporting;
drop table if exists AirportList;
drop table if exists Aircraft_Inventory;
drop table if exists CarrierDecode;

select '---------------------------------------------------------------------------------------' as '';
select 'Create AirportList' as '';

create table AirportList
(
			ORIGIN_AIRPORT_ID decimal(5) primary key,
			DISPLAY_AIRPORT_NAME char(50),
			ORIGIN_CITY_NAME char(50),
			NAME char(100)
);
	
show warnings limit 10;
select '---------------------------------------------------------------------------------------' as '';


select '---------------------------------------------------------------------------------------' as '';
select 'Create Aircraft_Inventory' as '';

create table Aircraft_Inventory
(
			MANUFACTURE_YEAR decimal(4),
			TAIL_NUM char(10) primary key,
			NUMBER_OF_SEATS int
);

show warnings limit 10;
select '---------------------------------------------------------------------------------------' as '';



select '---------------------------------------------------------------------------------------' as '';
select 'Create CarrierDecode' as '';

create table CarrierDecode
(
			AIRLINE_ID decimal(5),
			OP_UNIQUE_CARRIER char(5),
			CARRIER_NAME char(100),
			primary key(OP_UNIQUE_CARRIER,CARRIER_NAME)
);
	
show warnings limit 10;
select '---------------------------------------------------------------------------------------' as '';
	

select '---------------------------------------------------------------------------------------' as '';
select 'Create AirportCoordinates' as '';

create table AirportCoordinates
(
			ORIGIN_AIRPORT_ID decimal(5),
			LATITUDE float,
			LONGITUDE float,
			primary key(ORIGIN_AIRPORT_ID)
);

show warnings limit 10;
select '---------------------------------------------------------------------------------------' as '';


select '---------------------------------------------------------------------------------------' as '';
select 'Create Airport_Weather' as '';

create table Airport_Weather
(
			ORIGIN_AIRPORT_ID decimal(5),
			STATION char(20),
			Weather_DATE DATE,
			AWND float,PGTM float,PRCP float,SNOW float,SNWD float,TAVG float,TMAX float,TMIN float,WDF2 float,
			WDF5 float,WSF2 float,WSF5 float,WT01 float,WT02 float,WT03 float,WT04 float,WT05 float,WT06 float,
			WT07 float,WT08 float,WT09 float,WESD float,WT10 float,PSUN float,TSUN float,SN32 float,SX32 float,
			TOBS float,WT11 float,WT18 float,
			primary key(ORIGIN_AIRPORT_ID,STATION,Weather_DATE),
			foreign key (ORIGIN_AIRPORT_ID) references AirportList(ORIGIN_AIRPORT_ID)
);

show warnings limit 10;
select '---------------------------------------------------------------------------------------' as '';


select '---------------------------------------------------------------------------------------' as '';
select 'Create CARRIER_SUMMARY_AIRPORT_ACTIVITY' as '';

create table CARRIER_SUMMARY_AIRPORT_ACTIVITY
(
			OP_UNIQUE_CARRIER char(5),
			CARRIER_NAME char(100),
			ORIGIN_AIRPORT_ID decimal(5),
			SERVICE_CLASS char(1),
			REV_ACRFT_DEP_PERF_510 int,
			REV_PAX_ENP_110 int,
			primary key(OP_UNIQUE_CARRIER,CARRIER_NAME,ORIGIN_AIRPORT_ID,SERVICE_CLASS),
			foreign key (OP_UNIQUE_CARRIER) references CarrierDecode(OP_UNIQUE_CARRIER),
			foreign key (ORIGIN_AIRPORT_ID) references AirportList(ORIGIN_AIRPORT_ID)
);


show warnings limit 10;
select '---------------------------------------------------------------------------------------' as '';



select '---------------------------------------------------------------------------------------' as '';
select 'Create Employees' as '';

create table Employees
(
			YEAR decimal(4),
			AIRLINE_ID decimal(5),
			OP_UNIQUE_CARRIER char(5),
			CARRIER_NAME char(100),
			ENTITY char(5),
			GENERAL_MANAGE int,
			PILOTS_COPILOTS int,
			OTHER_FLT_PERS int,
			PASS_GEN_SVC_ADMIN int,
			MAINTENANCE int,
			ARCFT_TRAF_HANDLING_GRP1 int,
			GEN_ARCFT_TRAF_HANDLING int,
			AIRCRAFT_CONTROL int,
			PASSENGER_HANDLING int,
			CARGO_HANDLING int,
			TRAINEES_INTRUCTOR int,
			STATISTICAL int,
			TRAFFIC_SOLICITERS int,
			OTHER int,
			TRANSPORT_RELATED int,
			TOTAL int,
			primary key(OP_UNIQUE_CARRIER,CARRIER_NAME,ENTITY),
			foreign key (OP_UNIQUE_CARRIER) references CarrierDecode(OP_UNIQUE_CARRIER)
);

select '---------------------------------------------------------------------------------------' as '';



select '---------------------------------------------------------------------------------------' as '';
select 'Create OntimeReporting' as '';

create table OntimeReporting
( MONTH INT,
	DAY_OF_MONTH INT,
	DAY_OF_WEEK INT,
	OP_UNIQUE_CARRIER CHAR ( 5 ),
	TAIL_NUM CHAR ( 10 ),
	OP_CARRIER_FL_NUM DECIMAL ( 4 ),
	ORIGIN_AIRPORT_ID DECIMAL ( 5 ),
	ORIGIN CHAR ( 5 ),
	ORIGIN_CITY_NAME CHAR ( 50 ),
	DEST_AIRPORT_ID DECIMAL ( 5 ),
	DEST CHAR ( 5 ),
	DEST_CITY_NAME CHAR ( 50 ),
	CRS_DEP_TIME INT,
	DEP_TIME INT,
	DEP_DELAY_NEW INT,
	DEP_DEL15 INT,
	DEP_TIME_BLK CHAR ( 10 ),
	CRS_ARR_TIME INT,
	ARR_TIME INT,
	ARR_DELAY_NEW INT,
	ARR_TIME_BLK CHAR ( 10 ),
	CANCELLED DECIMAL ( 1 ),
	CANCELLATION_CODE CHAR ( 5 ),
	CRS_ELAPSED_TIME INT,
	ACTUAL_ELAPSED_TIME INT,
	DISTANCE INT,
	DISTANCE_GROUP INT,
	CARRIER_DELAY INT,
	WEATHER_DELAY INT,
	NAS_DELAY INT,
	SECURITY_DELAY INT,
	LATE_AIRCRAFT_DELAY INT,
	PRIMARY KEY ( MONTH, DAY_OF_MONTH, OP_UNIQUE_CARRIER, TAIL_NUM, ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID ),
	FOREIGN KEY ( OP_UNIQUE_CARRIER ) REFERENCES CarrierDecode ( OP_UNIQUE_CARRIER ),
	FOREIGN KEY ( TAIL_NUM ) REFERENCES Aircraft_Inventory ( TAIL_NUM ),
	FOREIGN KEY ( ORIGIN_AIRPORT_ID ) REFERENCES AirportList ( ORIGIN_AIRPORT_ID ),
	FOREIGN KEY ( DEST_AIRPORT_ID ) REFERENCES AirportList ( ORIGIN_AIRPORT_ID ) 
);
	

select '---------------------------------------------------------------------------------------' as '';
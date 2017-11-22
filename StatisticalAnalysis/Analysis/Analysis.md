**selecting database in hive**<br />
%sql
use road_safety

**per year wise number of accidents for PedestrianRoadMaintenanceWorker by per types of car**<br />
%sql
select make_model_2013_15.accyr,lookup_vehicle_types.label,count(*)
from road_safety.casualities_2013_15,lookup_vehicle_types,road_safety.make_model_2013_15 
where casualities_2013_15.Accident_Index=make_model_2013_15.Accident_Index and make_model_2013_15.Vehicle_Type = lookup_vehicle_types.code and casualities_2013_15.Pedestrian_Road_Maintenance_Worker = 1
group by make_model_2013_15.accyr,lookup_vehicle_types.label

**year wise casualties for Pedestrian_Road_Maintenance_Worker per vehicle type using hiveContext**<br />
val hiveContext= new org.apache.spark.sql.hive.HiveContext(sc)<br />

val h = hiveContext.sql("select make_model_2013_15.accyr,VEHICLE_LOOKUP.label,count(Pedestrian_Road_Maintenance_Worker) from road_safety.casualities_2013_15,VEHICLE_LOOKUP,road_safety.make_model_2013_15 where casualities_2013_15.Accident_Index=make_model_2013_15.Accident_Index and make_model_2013_15.Vehicle_Type = VEHICLE_LOOKUP.code group by make_model_2013_15.accyr,VEHICLE_LOOKUP.label")

**year wise number of casualties for per vehicle type**<br />

%sql
select make_model_2013_15.accyr,lookup_vehicle_types.label,count(*)
from casualities_2013_15,lookup_vehicle_types,make_model_2013_15,Age_Band
where casualities_2013_15.Accident_Index=make_model_2013_15.Accident_Index and make_model_2013_15.Vehicle_Type = lookup_vehicle_types.code
group by make_model_2013_15.accyr,lookup_vehicle_types.label

**year wise casulaties per age band**<br />
%sql
select accyr,Age_Band.label,count(*) as cnt
from casualities_2013_15,Age_Band
where casualities_2013_15.Age_Band_of_Casualty = Age_Band.code
group by accyr,Age_Band.label
order by accyr asc,cnt desc

**year wise number of fatals**<br />
%sql
select accyr,Casualty_severity.label as Label,count(*) as cnt
from casualities_2013_15,Casualty_severity
where casualities_2013_15.Casualty_Severity=Casualty_severity.code and Casualty_severity.label = "Fatal"
group by accyr,Casualty_Severity.label
order by cnt desc

**year wise number of fatals per gender**<br />

%sql
select accyr,Casualty_severity.label,Sex_of_Casualty.label,count(*) as cnt
from casualities_2013_15,Casualty_severity,Sex_of_Casualty
where casualities_2013_15.Casualty_Severity = Casualty_severity.code and casualities_2013_15.Sex_of_Casualty = Sex_of_Casualty.code and  Casualty_severity.label = "Fatal"
group by accyr,Casualty_severity.label,Sex_of_Casualty.label

**caualties per year per types of vehicle**<br />

%sql
select casualities_2013_15.accyr,vehicles2013_2015.Vehicle_Type,count(*) cnt
from casualities_2013_15,vehicles2013_2015,lookup_vehicle_types
where casualities_2013_15.accident_index=vehicles2013_2015.accident_index 
group by casualities_2013_15.accyr,vehicles2013_2015.Vehicle_Type
order by cnt desc

**year wise top 5 makes involved in the accidents**<br />

%sql
select accyr,make,count(make) as cnt
from make_model_2013_15
group by accyr,make
order by cnt desc limit 5

**year wise sex of driver faced accident**<br />

%sql
select accyr,Sex_of_Driver_tab.label, count(*) as cnt 
from make_model_2013_15,Sex_of_Driver_tab
where make_model_2013_15.Sex_of_Driver=Sex_of_Driver_tab.code
group by accyr,Sex_of_Driver_tab.label
order by cnt desc

**Top five makes who have been in accident in 2013**<br />

%sql
select make as make_2013,Count(*) as ct 
from road_safety.make_model_2013_15 
where accyr = 2013 
group by make 
order by ct desc limit 5

**per year number of accident per age band**<br />
%sql
select make_model_2013_15.accyr,Age_Band.label as Label,count(*) as cnt
from make_model_2013_15,Age_Band
where make_model_2013_15.Age_Band_of_Driver=Age_Band.code
group by make_model_2013_15.accyr,Age_Band.label
order by cnt desc

**per year per age number of fatal accidents**<br />

%sql
select make_model_2013_15.accyr,Age_Band.label as Label,count(*) as cnt
from make_model_2013_15,Age_Band
where make_model_2013_15.Age_Band_of_Driver=Age_Band.code
group by make_model_2013_15.accyr,Age_Band.label
order by cnt desc

**per year per age number of fatal accidents** <br />

%sql
select casualities_2013_15.accyr,Age_Band.label as LABEL,Casualty_severity.label as LABEL,count(*) as cnt
from casualities_2013_15,make_model_2013_15,Age_Band,Casualty_severity
where casualities_2013_15.Accident_Index = make_model_2013_15.Accident_Index and make_model_2013_15.Age_Band_of_Driver = Age_Band.code and casualities_2013_15.Casualty_Severity=Casualty_severity.code and Casualty_severity.label = "Fatal"
group by casualities_2013_15.accyr,Age_Band.label,Casualty_severity.label
order by cnt desc

**per yer number of fatal accident per make where driver of age less than 5 years**<br />

%sql
select casualities_2013_15.accyr,Age_Band.label as LABEL,Casualty_severity.label as LABEL,lookup_Vehicle_Type.label as VEHICLE_TYPE,count(*) as cnt
from casualities_2013_15,make_model_2013_15,Age_Band,Casualty_severity,lookup_Vehicle_Type
where casualities_2013_15.Accident_Index = make_model_2013_15.Accident_Index and make_model_2013_15.Age_Band_of_Driver = Age_Band.code and casualities_2013_15.Casualty_Severity=Casualty_severity.code and make_model_2013_15.Age_Band_of_Driver =  Age_Band.code and make_model_2013_15.Vehicle_Type=lookup_Vehicle_Type.code and casualities_2013_15.Casualty_severity = "1" and make_model_2013_15.Age_Band_of_Driver= "1"
group by casualities_2013_15.accyr,Age_Band.label,Casualty_severity.label,make_model_2013_15.Vehicle_Type,lookup_Vehicle_Type.label

**Top ten cities with most accidents in 2015**<br />

%sql 
select city, count(*) cnt
from location
group by city
order by cnt desc limit 10

**top five cities are in accidents**<br />

%sql
select city, count(*) cnt
from accidents_2015_city
group by city
order by cnt desc limit 5

**Area(rural/urban) wise Accidents in 2015**<br />

%sql
select lookups_2015_area.label,count(*) count
from lookups_2015_area,accidents_2015_city
where accidents_2015_city.Urban_or_Rural_Area=lookups_2015_area.code
group by lookups_2015_area.label

**year wise how many female driver in accident**<br />

%sql
select accyr,Sex_of_Driver_tab.label, count(*) cnt
from vehicles2013_2015,Sex_of_Driver_tab
where vehicles2013_2015.Sex_of_Driver = Sex_of_Driver_tab.code and vehicles2013_2015.Sex_of_Driver=2
group by accyr,Sex_of_Driver_tab.label
order by cnt desc

**year wise casualties per Home_Area_Type per vehicle type**<br />
%sql
select casualities_2013_15.accyr,Home_Area_Type.label,lookup_Vehicle_Type.label, count(*)
from casualities_2013_15,vehicles2013_2015,Home_Area_Type,lookup_Vehicle_Type
where casualities_2013_15.Accident_Index=vehicles2013_2015.Accident_Index and casualities_2013_15.Casualty_Home_Area_Type=Home_Area_Type.code and vehicles2013_2015.Vehicle_Type=lookup_Vehicle_Type.code
group by casualities_2013_15.accyr,Home_Area_Type.label,lookup_Vehicle_Type.label

**Number of casualties per age in 2015**<br />
%sql
select Age_of_Casualty, count(*)
from casualties_2015
group by Age_of_Casualty
order by count(*) desc

**Top ten cities with most accidents in 2015**<br />

%sql 
select city, count(*) cnt
from location
group by city
order by cnt desc limit 10

**number of casualties at the age of 19 in Birmingham**<br />
%sql
select count(location.Accident_Index)
from location,casualties_2015
where location.Accident_Index=casualties_2015.Accident_Index and casualties_2015.Age_of_Casualty=19 and location.city='Birmingham'


__NOTE__<br />
All this analysis will work as a Spark Job. It can be seen in the Spark UI. If we run all the spark sql querries on hive it will run on TEZ.and can be seen on spark UI.

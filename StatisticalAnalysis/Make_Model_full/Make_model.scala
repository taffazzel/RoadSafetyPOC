//creating dataframe for each year
val make_model_2013df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2013/Make_and_Model_2013.csv")
val make_model_2014df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2014/Make_and_Model_2014.csv")
val make_model_2015df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2015/2015_Make_Model.csv")

//setting up properties 
val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
hiveContext.setConf("hive.exec.dynamic.partition", "true")
hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

//union operation to get a single dataframe(DF) out of three DF
val temp_df_2013_14 = make_model_2013df.unionAll(make_model_2014df)
val full_df_2013_15 = temp_df_2013_14.unionAll(make_model_2015df)
full_df_2013_15.count()
full_df_2013_15.printSchema()

//performing rename operation to maintain the naming convention

var full_df_temp1 = full_df_2013_15.withColumnRenamed("Engine_Capacity_(CC)", "Engine_Capacity")
var full_df_temp2 = full_df_temp1.withColumnRenamed("Vehicle_Location-Restricted_Lane", "Vehicle_Location_Restricted_Lane")
val full_df_fin = full_df_temp2.withColumnRenamed("Acc_Index", "Accident_Index")

 // spark sql interpreter
%sql
use road_safety

//creating a temporary table ORC format, out of unioned DF 

full_df_fin.write.format("orc").saveAsTable("make_model_2013_15_temp")

//creating a table
%sql
create table make_model_2013_15(Accident_Index string, Vehicle_Reference int,Vehicle_Type int,Towing_and_Articulation int,Vehicle_Manoeuvre int,Vehicle_Location_Restricted_Lane int,Junction_Location int,Skidding_and_Overturning int,Hit_Object_in_Carriageway int,Vehicle_Leaving_Carriageway int,Hit_Object_off_Carriageway int,1st_Point_of_Impact int,Was_Vehicle_Left_Hand_Drive int,Journey_Purpose_of_Driver int,Sex_of_Driver int,Age_Band_of_Driver int,Engine_Capacity int,Propulsion_Code int,Age_of_Vehicle int,Driver_IMD_Decile int,Driver_Home_Area_Type int,make string,model string) partitioned by (accyr int) row format delimited fields terminated by ',' stored as orc

//inserting all data to make_model_2013_15 from make_model_2013_15_temp

insert overwrite table road_safety.make_model_2013_15 partition(accyr) select Accident_Index,Vehicle_Reference,Vehicle_Type,Towing_and_Articulation,Vehicle_Manoeuvre,Vehicle_Location_Restricted_Lane ,Junction_Location,Skidding_and_Overturning,Hit_Object_in_Carriageway,Vehicle_Leaving_Carriageway,Hit_Object_off_Carriageway,1st_Point_of_Impact,Was_Vehicle_Left_Hand_Drive,Journey_Purpose_of_Driver,Sex_of_Driver,Age_Band_of_Driver,Engine_Capacity,Propulsion_Code,Age_of_Vehicle,Driver_IMD_Decile,Driver_Home_Area_Type,make,model,accyr from road_safety.make_model_2013_15_temp

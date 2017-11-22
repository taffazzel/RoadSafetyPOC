var Vehicles_2013df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2013/Vehicles_2013.csv")
var Vehicles_2014df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2014/Vehicles_2014.csv")
var Vehicles_2015df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2015/Vehicles_2015.csv")

//droping one cloumn
val Vehicles_2015df_temp = Vehicles_2015df.drop(Vehicles_2015df.col("Vehicle_IMD_Decile"))
//setting some properties which allow to create a partitioned table
//creating hiveContext
val hiveContext= new org.apache.spark.sql.hive.HiveContext(sc)
hiveContext.setConf("hive.exec.dynamic.partition", "true")
hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

//changing one column name
val Vehicles_2013df_temp = Vehicles_2013df.withColumnRenamed("Acc_Index", "Accident_Index")

// filling missing data. Details have been explined in readme

val x= -1
val a= 2013
val b= 2014
val c= 2015
val accyar_2013df = udf(() => a)
val accyar_2014df = udf(() => b)
val accyar_2015df = udf(() => c)
val age_return_driver = udf(() => x)
val Vehicles_2013df_temp_2 = Vehicles_2013df_temp.withColumn("Age_of_Driver", age_return_driver())
val Vehicles_2013df_added = Vehicles_2013df_temp_2.withColumn("accyr",accyar_2013df())
val Vehicles_2014df_added = Vehicles_2014df.withColumn("accyr",accyar_2014df())
val Vehicles_2015df_added = Vehicles_2015df_temp.withColumn("accyr",accyar_2015df())

//performing Union operation to make a single dataframe
val Vehicles_2013_14df = Vehicles_2013df_added.unionAll(Vehicles_2014df_added)
val Vehicles_2013_15df = Vehicles_2013_14df.unionAll(Vehicles_2015df_added)
Vehicles_2013_15df.show()

//renaming some columns to maintain naming convention
var Vehicles_2013_15df_temp1 = Vehicles_2013_15df.withColumnRenamed("Engine_Capacity_(CC)", "Engine_Capacity")
val Vehicles_2013_15df_temp2 = Vehicles_2013_15df_temp1.withColumnRenamed("Vehicle_Location-Restricted_Lane", "Vehicle_Location_Restricted_Lane")
Vehicles_2013_15df_finall = Vehicles_2013_15df_temp2.withColumnRenamed("Was_Vehicle_Left_Hand_Drive?", "Was_Vehicle_Left_Hand_Drive")
Vehicles_2013_15df_finall.printSchema()



// %sql means sparksql interpreter which allows developer to write code in spark sql and it creates spark job. can be checked in spark UI
%sql
use road_safety
//creating a table under road_safety db which will have all the datasets in Vehicles_2013_15df_finall dataframe
Vehicles_2013_15df_finall.write.format("orc").saveAsTable("Vehicles_2013_15tb_temp")

// creating table in hive under road_safety db 
create table vehicles2013_2015(Accident_Index string,Vehicle_Reference int,Vehicle_Type int,Towing_and_Articulation int,Vehicle_Manoeuvre int,Vehicle_Location_Restricted_Lane int,Junction_Location int,Skidding_and_Overturning int,Hit_Object_in_Carriageway int,Vehicle_Leaving_Carriageway int,Hit_Object_off_Carriageway int,1st_Point_of_Impact int,Was_Vehicle_Left_Hand_Drive int,Journey_Purpose_of_Driver int, Sex_of_Driver int, Age_Band_of_Driver int,Engine_Capacity int,Propulsion_Code int,Age_of_Vehicle int,Driver_IMD_Decile int,Driver_Home_Area_Type int, Age_of_Driver int) partitioned by (accyr int) row format delimited fields terminated by ',' stored as orc

//now inserting all the data from  Vehicles_2013_15tb_temp to vehicles2013_2015
%sql
insert overwrite table road_safety.vehicles2013_2015 partition(accyr) select Accident_Index,Vehicle_Reference,Vehicle_Type,Towing_and_Articulation,Vehicle_Manoeuvre,Vehicle_Location_Restricted_Lane,Junction_Location,Skidding_and_Overturning,Hit_Object_in_Carriageway,Vehicle_Leaving_Carriageway,Hit_Object_off_Carriageway,1st_Point_of_Impact,Was_Vehicle_Left_Hand_Drive,Journey_Purpose_of_Driver,Sex_of_Driver,Age_Band_of_Driver,Engine_Capacity,Propulsion_Code,Age_of_Vehicle,Driver_IMD_Decile,Driver_Home_Area_Type,Age_of_Driver,accyr from road_safety.Vehicles_2013_15tb_temp
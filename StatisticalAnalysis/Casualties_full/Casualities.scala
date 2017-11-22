// creating dataframes
val Casualities_2013df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2013/Casualties_2013.csv")
val Casualities_2014df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2014/Casualties_2014.csv")
val Casualities_2015df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2015/Casualties_2015.csv")

// renaming and dropping one column

val Casualities_2013df_ren = Casualities_2013df.withColumnRenamed("Acc_Index", "Accident_Index")
val Casualities_2015df_rem = Casualities_2015df.drop(Casualities_2015df.col("Casualty_IMD_Decile"))
Casualities_2015df_rem.printSchema()

// adding coloumn and filling up with some data to make a uniform number of columns
val x = -1
val y= 2013
val z= 2014
val c= 2015
val year_return_2013 = udf(() => y)
val year_return_2014 = udf(() => z)
val year_return_2015 = udf(() => c)
val age_return = udf(() => x)
val Casualities_2013df_final = Casualities_2013df_ren.withColumn("Age_of_Casualty", age_return())
val Casualities_2013df_final_new = Casualities_2013df_final.withColumn("accyr", year_return_2013())
val Casualities_2014df_final = Casualities_2014df.withColumn("accyr", year_return_2014())
val Casualities_2015df_final = Casualities_2015df_rem.withColumn("accyr", year_return_2015())
Casualities_2013df_final.printSchema()

//performing union operation
val temp_df = Casualities_2013df_final_new.unionAll(Casualities_2014df_final)
val Casulalities_2013_15df=temp_df.unionAll(Casualities_2015df_final)

//setting up hive properties
hiveContext.setConf("hive.exec.dynamic.partition", "true")
hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

//creating a table in hive out of unioned DF
Casulalities_2013_15df.write.format("orc").saveAsTable("Casualities_2013_15_orc")


//Spark-sql interpreter
%sql
use road_safety
//creating table

%sql
create table casualities_2013_15_temp(Accident_Index string,Vehicle_Reference int,Casualty_Reference int,Casualty_Class int,Sex_of_Casualty int,Age_Band_of_Casualty int,Casualty_Severity int, Pedestrian_Location int, Pedestrian_Movement int,Car_Passenger int,Bus_or_Coach_Passenger int,Pedestrian_Road_Maintenance_Worker int,Casualty_Type int,Casualty_Home_Area_Type int, Age_of_Casualty int) partitioned by (accyr int) row format delimited fields terminated by ',' stored as orc

// inserting all data from Casualities_2013_15_orc to casualities_2013_15_temp
insert overwrite table road_safety.casualities_2013_15_temp partition(accyr) select Accident_Index,Vehicle_Reference,Casualty_Reference,Casualty_Class,Sex_of_Casualty,Age_Band_of_Casualty,Casualty_Severity, Pedestrian_Location, Pedestrian_Movement,Car_Passenger,Bus_or_Coach_Passenger,Pedestrian_Road_Maintenance_Worker,Casualty_Type,Casualty_Home_Area_Type, Age_of_Casualty, accyr from road_safety.Casualities_2013_15_orc

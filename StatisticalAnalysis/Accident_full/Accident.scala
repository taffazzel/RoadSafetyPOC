
//this is creating individual dataframe for each year
val Accidents_2013df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/road_safety/datasets/2013/Accidents_2013_loc.csv")
val Accidents_2014df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").load("/road_safety/datasets/2014/Accidents_2014_loc.csv")
val Accidents_2015df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").load("/road_safety/datasets/2015/Accidents_2015_loc.csv")

// creating hiveContext using given sc (by default sc is SparkContext) and setting properties which allows us to perform partion operation
val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
hiveContext.setConf("hive.exec.dynamic.partition", "true")
hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

//performing Union operartion to make a dataframe out of three dataframe
val Accidents_temp_df = Accidents_2013df.unionAll(Accidents_2014df)
val Accidents_full_df = Accidents_temp_df.unionAll(Accidents_2015df)
Accidents_full_df.count()

// %sql represents spark sql interpreter.this creates a spark job(can be seen on spark UI. This uses hive databases)
%sql
use road_safety
//creating permanent table in hive under road_safety database orc formated and put all the datasets in dataframe to "accidents_2013_15_temp"
Accidents_full_df.write.format("orc").saveAsTable("accidents_2013_15_temp")
//creating a partitioned table in road_safety db
create table Accidents_2013_15(id int,Accident_Index string,Longitude string,Latitude string,Urban_or_Rural_Area int) partitioned by(city string) row format delimited fields terminated by ',' stored as orc;

// transfer all the data to a partitioned and orc formated table from accidents_2013_15_temp
insert overwrite table Accidents_2013_15 partition(city) select id,Accident_Index,Longitude,Latitude,Urban_or_Rural_Area,city from accidents_2013_15_temp


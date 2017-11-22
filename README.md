# `RoadSafetyPOC`
# This is ETL centric proof of Concept (POC) for analyzing UK Road Safety and Accident statistics for three years 2013-15.

We have used this open datasource for road accident: `https://data.gov.uk/dataset/road-accidents-safety-data`
The datasource mainly gives us four different dataset :

* Accident details
* Vehicle details
* Make and Model
* Casulaties

In the datset,different years of data available. For our statistical analysis purpose, we have used 2013-15<br />
Also, the data source gives us the column definition by lookup.

Our goal is to achieve the statistical analysis on UK road accident dataset using one of the fastest bigdata analysis framework Spark on yarn.<br />

**To Know**<br />

* We downloaded the all the required dataset like Accident, vehicles, lookups etc.
* we dumped into HDFS.
* we dealt Accident dataset differently for getting city name.
	* First we imported the dataset to a database in mysql.
	* A python code getLocation.py , which uses the required field for reverse Geocoding to get the name of the cities.
	* We update the database table with the name of the cities for each accident.
	* we then exported the mysql table to CSV.
	* Then again dumped back to HDFS.

* For Accident, and Make and Model the number of fields in the dataset are same for three years.
* For Vehicles and Casualties number of fields were not same.Thus, data preparation phase has been in the place.Details have been mentioned in the corresponding folder.
* After all the datasets have been prepared, proceeded for the Analysis.
* For the Analysis we used Spark on yarn.
* Some of the programs were using SqlContext and some of them were using HiveContext. To get better graphical analyisis,"%sql" was used more which is spark sql interpreter.
* All the programs were written on zeppelin notebook. Thus all the programs should be working fine on Spark terminal except the codes were written using spark sql interpreter(%sql). In order to make this work, the analytical querries should be passed through either using hiveContext or sqlContext(in spark2.0 both have to be created explicitly using 'sc' object of SparkContext and by default given) calling sql function(hiveContext.sql("query")/sqlContext.sql("query"))
* In spark 2.0, by default 'spark' which is object of SparkSession, is also given. So we can sql function with that object and pass the querries as mentioned in the previous line. It should work.

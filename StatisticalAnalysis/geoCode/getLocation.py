#!/usr/bin/python
import io
import reverse_geocoder as rg
import mysql.connector
import traceback
from pygeocoder import Geocoder

from opencage.geocoder import OpenCageGeocode
key = 'a0719ed989636ccb8b9d47133073f62b'
geocoder = OpenCageGeocode(key)
db = mysql.connector.connect(host='localhost', user='root', passwd='', db='roadsafety')

# you must create a Cursor object. It will let
# you execute all the queries you need
cursor = db.cursor()
cursor1 = db.cursor()

#sql = "SELECT * FROM road_accident where id > 123382 limit 20000"
sql = "select * from Accidents_fields_2015"
#sql2 = "select * from  accident_data limit 3"
#update = ("update road_accident set city = %s where Accident_Index = %" %(city,Accident_Index))

try:
   # Execute the SQL command
   cursor.execute(sql)
   # Fetch all the rows in a list of lists.
   results = cursor.fetchall()
   for row in results:
        if(row[2]!=""):
                id = row[0]
                Accident_Index = row[1]
                Day_of_Week = row[2]
                Time_of = row[3]
                Longitude = row[4]
                Latitude = row[5]
                city_name = row[6]
                Accident_Severity = row[7]
                Weather_Conditions = row[8]
                Speed_limit = row[9]
                Road_Type = row[10]
                Road_Surface_Conditions = row[11]
                Urban_or_Rural_Area = row[12]
                #Now print fetched result
                print "Here is the id=%d,Accident_Index=%s,Day_of_Week=%s,Time_of=%s,Longitude=%s,Latitude=%s,city=%s,Accident_Severity=%d,Weather_Conditions=%d,Speed_limit=%d,Road_Type=%d,Road_Surface_Conditions=%d,Urban_or_Rural_Area=%d" % (id, Accident_Index,Day_of_Week,Time_of,Longitude, Latitude, city_name,Accident_Severity,Weather_Conditions,Speed_limit,Road_Type,Road_Surface_Conditions,Urban_or_Rural_Area )
                location = (Latitude,Longitude)
                result = rg.search(location)
                #print result
                city_result = result[0]['name']
                print city_result
                print Accident_Index
                print id
                
				
                cursor.execute("""UPDATE Accidents_fields_2015 SET city=%s WHERE id=%s""",(city_result,id))
				               
                
except:
        print "Error: unable to fecth data"
        traceback.print_exc()

# disconnect from server
db.close()
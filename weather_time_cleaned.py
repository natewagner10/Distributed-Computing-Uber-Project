

weather = sc.textFile("uber_weather.csv")

weather = weather.filter(lambda x: x!= 'datetime,lat,lng,base,humidity,wind,temp,description')


def clean_weather(line):
    return line.split(",")

weather_clean = weather.map(clean_weather)

from datetime import date
import datetime
from dateutil import parser

def fix_time(line):
    return(parser.parse(line[0]), float(line[1]), float(line[2]), line[3], float(line[4]), float(line[5]), float(line[6]), line[7])

weather = weather_clean.map(fix_time)

def adddays(line):
    return line[0], line[0].day, line[0].month, line[1], line[2], line[3], line[4], line[5], line[6], line[7]

weather_days = weather.map(adddays)


def rain(line):
    if line[9] == "scattered clouds" or line[9] == "sky is clear" or line[9] == "broken clouds" or line[9] == "haze" or line[9] == "few clouds" or line[9] == "overcast clouds" or line[9] == "mist" or line[9] == "fog" or line[9] == "dust" or line[9] == "smoke":
        dummy = "no rain"
    else:
        dummy = "rain"
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], dummy


weather_days_rain = weather_days.map(rain)

def floater(line):
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10] 


weather1 = weather_days_rain.map(floater)
weather_df = weather1.toDF()


weather_df = weather_df.selectExpr("_1 as datetime1", "_2 as day", "_3 as month", "_4 as lat", "_5 as lng",  "_6 as base", "_7 as humidity", "_8 as wind", "_9 as temp", "_10 as desc", "_11 as rain")
weather_df.show(5)


weather_df.createOrReplaceTempView("uber2014")


test = sqlContext.sql("select day, month, rain, count(rain) number_trips from uber2014 group by day, month, rain order by day desc")
test.createOrReplaceTempView("counttrips")


average_trips_per_day = sqlContext.sql("select mean(number_trips) mean_num_trips from counttrips")
average_trips_per_day_with_rain = sqlContext.sql("select rain, mean(number_trips) mean_num_trips from counttrips group by rain")
average_trips_per_month_with_rain = sqlContext.sql("select month, rain, mean(number_trips) mean_num_trips from counttrips group by month, rain order by month desc")










weather = sc.textFile("weather.csv")

weather = weather.filter(lambda x: x!= 'date_time,humidity,wind,temp,description')

def clean_weather(line):
    return line.split(",")

    
weather_clean = weather.map(clean_weather)


from datetime import date
import datetime

def fix_time(line):
    return (datetime.datetime.strptime(line[0], '%m/%d/%y %H:%M'), line[1], line[2], line[3], line[4])
    
weather_clean_time = weather_clean.map(fix_time)




def getTime(line):
    return line[0], line[0].month, line[0].day, line[0].hour, line[1], line[2], line[3], line[4]

weather_clean_time2 = weather_clean_time.map(getTime)

def getTimeUber(line):
    return line[0], line[0].month, line[0].day, line[0].hour, line[1], line[2], line[3]

full_data_uber_2014_time = full_data_uber_2014.map(getTimeUber)

full_data_uber_2014_time_df = full_data_uber_2014_time.toDF()
weather_clean_time2_df = weather_clean_time2.toDF()


full_data_uber_2014_time_df = full_data_uber_2014_time_df.selectExpr("_1 as datetime", "_2 as month", "_3 as day", "_4 as hour", "_5 as lat", "_6 as lng", "_7 as base")
weather_clean_time2_df = weather_clean_time2_df.selectExpr("_1 as datetime1", "_2 as month1", "_3 as day1", "_4 as hour1", "_5 as humidity", "_6 as wind", "_7 as temp", "_8 as description") 


left_join = full_data_uber_2014_time_df.join(weather_clean_time2_df, (full_data_uber_2014_time_df.month == weather_clean_time2_df.month) & (full_data_uber_2014_time_df.day == weather_clean_time2_df.day) & (full_data_uber_2014_time_df.hour == weather_clean_time2_df.hour), how='left_outer') 

left_join = left_join.drop('datetime1')
left_join = left_join.drop('month1')
left_join = left_join.drop('day1')
left_join = left_join.drop('hour1')
left_join.show(5)

#left_join.toPandas().to_csv('uber_data_with_weather.csv')








def mapper(line):
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], 

weather_features = weather1.map(mapper)

weather_features_df = weather_features.toDF()




test1 = test.withColumn("features",udf_foo("features"))

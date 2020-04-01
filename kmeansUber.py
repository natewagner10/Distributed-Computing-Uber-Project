
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=2, seed=1)


def mapper(line):
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], 

weather_features = weather1.map(mapper)

weather_features_df = weather_features.toDF()
weather_df = weather_features_df.selectExpr("_1 as datetime1", "_2 as day", "_3 as month", "_4 as lat", "_5 as lng",  "_6 as base", "_7 as humidity", "_8 as wind", "_9 as temp", "_10 as desc", "_11 as rain", "_12 as latlng", "_13 as borough", "_14 as features")



test1 = weather_df.withColumn("features",udf_foo("features"))
test1.printSchema()

model = kmeans.fit(test1.select('features'))

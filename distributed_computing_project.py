#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 12:49:49 2020

@author: natewagner
"""


# load in data

uber_apr14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-apr14.csv")
uber_may14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-may14.csv")
uber_june14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-jun14.csv")
uber_july14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-jul14.csv")
uber_aug14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-aug14.csv")
uber_sep14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-sep14.csv")
uber_janjun15 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-janjune-15.csv")

# 2014 data is all in the same format, 2015 is a little different 


# union 2014 data 
full_data_uber_2014 = sc.union([uber_apr14, uber_may14, uber_june14, uber_july14, uber_aug14, uber_sep14])

# remove headers
full_data_uber_2014 = full_data_uber_2014.filter(lambda x: x!= '"Date/Time","Lat","Lon","Base"')

# to structure each line
def string_split(line):
    return line.split(",")

full_data_uber_2014 = full_data_uber_2014.map(string_split)












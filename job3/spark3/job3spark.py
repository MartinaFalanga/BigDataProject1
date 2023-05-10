#!/usr/bin/env python3

"""spark application"""

import argparse

from pyspark.sql import SparkSession

import re

import time



regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"



parser = argparse.ArgumentParser()

parser.add_argument("--input_path", type=str, help="Input file path")

parser.add_argument("--output_path", type=str, help="Output folder path")



args = parser.parse_args()

input_filepath, output_filepath = args.input_path, args.output_path


spark = SparkSession \

    .builder \

    .appName("Job3 Spark") \

    .getOrCreate()



rdd = spark.sparkContext.textFile("file:///home/martina/Scaricati/PrimoProgettoBigData/dataset_clean.csv").cache()



# remove csv header

header = rdd.first()

removedHeaderRDD = rdd.filter(lambda line: line != header)



# Filter reviews with score >= 4 (use index 5 for Score)

filteredScoreRDD = removedHeaderRDD.filter(lambda line: int(re.split(regex, line)[5]) >= 4)



# Create (UserId, ProductId) pairs

user2ProductsRDD = filteredScoreRDD.map(lambda line: (re.split(regex, line)[2], re.split(regex, line)[1]))



# Group by UserId and convert ProductId to a list

user2ProductsGroupedRDD = user2ProductsRDD.groupByKey().mapValues(list)



# Filter users with at least 3 products reviewed

filteredUsersRDD = user2ProductsGroupedRDD.filter(lambda x: len(x[1]) >= 3)



# Generate pairs of users (avoid duplicates) and calculate common products

# First, collect the filteredUsersRDD as a dictionary

filteredUsersDict = filteredUsersRDD.collectAsMap()



# Then, use a nested loop to calculate common products between user pairs

resultList = []

for user1, products1 in filteredUsersDict.items():

    for user2, products2 in filteredUsersDict.items():

        if user1 < user2:

            commonProducts = set(products1).intersection(products2)

            if len(commonProducts) >= 3:

                resultList.append((user1, user2, commonProducts))



# Convert the result list to an RDD

resultRDD = spark.sparkContext.parallelize(resultList)



resultRDD.saveAsTextFile("file:///home/martina/Scaricati/PrimoProgettoBigData/Job3/spark3/output")



print("Job completed successfully!")


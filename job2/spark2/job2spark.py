#!/usr/bin/env python3

"""spark application"""

from pyspark import SparkConf, SparkContext



# Inizializza la configurazione e la SparkContext

conf = SparkConf().setAppName("Job2 spark core")

sc = SparkContext(conf=conf)



data = sc.textFile("dataset/dataset_clean.csv")



def extract_fields(row):

    fields = row.split(",")

    user_id = fields[2]

    try:

        helpfulness_numerator = float(fields[3])

        helpfulness_denominator = float(fields[4])

        if helpfulness_denominator != 0:

            utility = helpfulness_numerator / helpfulness_denominator

        else:

            utility = 0

    except ValueError:

        utility = 0

    return (user_id, (utility, 1))



user_utility = data.map(extract_fields)



# Calcola la media dell'utilità per ogni utente

user_utility_sum_count = user_utility.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

user_utility_avg = user_utility_sum_count.mapValues(lambda v: v[0] / v[1])



# Ordina gli utenti in base all'apprezzamento (media dell'utilità) e in ordine alfabetico

sorted_users = user_utility_avg.sortBy(lambda x: (-x[1], x[0]), ascending=True)



output_path = ("/Job2/spark2/output")

sorted_users.saveAsTextFile(output_path)
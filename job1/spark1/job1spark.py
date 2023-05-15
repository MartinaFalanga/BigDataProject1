#!/usr/bin/env python3

"""spark application"""


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_unixtime, year
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType
import re

# Funzione per pulire il testo
def clean_text(text):

    return re.sub(r"[^a-zA-Z0-9\s]", "", text).lower()



# Funzione per estrarre le parole con almeno 4 caratteri
def extract_words(text):

    words = text.split(" ")

    return [word for word in words if len(word) >= 4]



conf = SparkConf().setAppName("Top 10 products per year")

sc = SparkContext(conf=conf)

spark = SparkSession(sc)



schema = StructType([

    StructField("Id", IntegerType(), True),

    StructField("ProductId", StringType(), True),

    StructField("UserId", StringType(), True),

    StructField("HelpfulnessNumerator", IntegerType(), True),

    StructField("HelpfulnessDenominator", IntegerType(), True),

    StructField("Score", IntegerType(), True),

    StructField("Time", LongType(), True),

    StructField("Text", StringType(), True)

])



data = spark.read.csv("file:///home/martina/Scaricati/PrimoProgettoBigData/dataset_clean.csv", header=False, schema=schema)



# Converte il campo Time in formato leggibile

data = data.withColumn("Year", year(from_unixtime("Time")))



# Pulisci il campo Text

data = data.withColumn("CleanText", udf(clean_text, StringType())("Text"))



# Estrai le parole con almeno 4 caratteri

data = data.rdd.map(lambda row: (row.Year, row.ProductId, row.CleanText)) \

    .flatMap(lambda x: [(x[0], x[1], word) for word in extract_words(x[2])])



# Conta le occorrenze delle parole

word_counts = data.map(lambda x: ((x[0], x[1], x[2]), 1)) \

    .reduceByKey(lambda a, b: a + b) \

    .map(lambda x: (x[0][0], x[0][1], x[0][2], x[1]))



# Trova le prime 5 parole per prodotto

top_words = word_counts.map(lambda x: ((x[0], x[1]), (x[2], x[3]))) \

    .groupByKey() \

    .mapValues(lambda x: sorted(list(x), key=lambda y: y[1], reverse=True)[:5]) \

    .map(lambda x: (x[0][0], x[0][1], "; ".join(["{} {}".format(word[0], word[1]) for word in x[1]])))



# Calcola il numero di recensioni per prodotto

review_counts = data.map(lambda x: ((x[0], x[1]), 1)) \

    .reduceByKey(lambda a, b: a + b) \

    .map(lambda x: (x[0][0], x[0][1], x[1]))



# Trova i primi 10 prodotti per anno

top_products = review_counts.map(lambda x: (x[0], (x[1], x[2]))) \

    .groupByKey() \

    .mapValues(lambda x: sorted(list(x), key=lambda y: y[1], reverse=True)[:10]) \

    .flatMapValues(lambda x: x) \

    .map(lambda x: (x[0], x[1][0]))



# Unisci i risultati

final_result = top_products.map(lambda x: ((x[0], x[1]), 1)) \

    .join(top_words.map(lambda x: ((x[0], x[1]), x[2]))) \

    .map(lambda x: (x[0][0], "{} [{}]".format(x[0][1], x[1][1]))) \

    .groupByKey() \

    .mapValues(lambda x: ", ".join(list(x))) \

    .filter(lambda x: x[0] is not None) \

    .sortByKey()




final_result.saveAsTextFile("file:///home/martina/Scaricati/PrimoProgettoBigData/Job1/spark1/output")
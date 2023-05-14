#!/usr/bin/env python3

"""spark sql application"""


from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import *


spark = SparkSession.builder \

    .appName("Job1 spark SQL") \

    .getOrCreate()


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



# Funzione per pulire il testo dai caratteri speciali

def clean_text(text):

    import re

    return re.sub(r"[^a-zA-Z0-9\s]", "", text).lower()



clean_text_udf = udf(clean_text, StringType())



# Pulisci il campo Text

data = data.withColumn("CleanText", clean_text_udf("Text"))



# Registra il DataFrame come tabella temporanea

data.createOrReplaceTempView("reviews")



# Query Spark SQL

query = """

WITH words AS (

    SELECT ProductId, Year, EXPLODE(SPLIT(CleanText, ' ')) AS Word

    FROM reviews

),

word_counts AS (

    SELECT Year, ProductId, Word, COUNT(*) AS WordCount

    FROM words

    WHERE LENGTH(Word) >= 4

    GROUP BY Year, ProductId, Word

),

top_words AS (

    SELECT Year, ProductId, Word, WordCount,

           ROW_NUMBER() OVER (PARTITION BY Year, ProductId ORDER BY WordCount DESC) AS RowNum

    FROM word_counts

),

top5_words AS (

    SELECT Year, ProductId, CONCAT_WS(' ', Word, WordCount) AS TopWords

    FROM top_words

    WHERE RowNum <= 5

),

grouped_top5_words AS (

    SELECT Year, ProductId, CONCAT('[', CONCAT_WS('; ', COLLECT_LIST(TopWords)), ']') AS TopWordsList

    FROM top5_words

    GROUP BY Year, ProductId

),

review_counts AS (

    SELECT Year, ProductId, COUNT(*) AS ReviewCount

    FROM reviews

    GROUP BY Year, ProductId

),

sorted_review_counts AS (

    SELECT Year, ProductId, ReviewCount,

           ROW_NUMBER() OVER (PARTITION BY Year ORDER BY ReviewCount DESC) AS RowNum

    FROM review_counts

),

top10_products AS (

    SELECT Year, ProductId

    FROM sorted_review_counts

    WHERE RowNum <= 10

),

grouped_results AS (

    SELECT t.Year, CONCAT(t.ProductId, ' ', g.TopWordsList) AS Result

    FROM top10_products t

    JOIN grouped_top5_words g ON t.Year = g.Year AND t.ProductId = g.ProductId

)

SELECT Year, CONCAT_WS(", ", COLLECT_LIST(Result)) AS Results

FROM grouped_results

GROUP BY Year

ORDER BY Year

"""

# Esegui la query e ottieni i risultati

result = spark.sql(query)



# Salva i risultati in un file

result.write.csv("file:///home/martina/Scaricati/PrimoProgettoBigData/Job1/sparksql/output", mode="overwrite", header=True)


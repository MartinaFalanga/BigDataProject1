#!/usr/bin/env python3

"""spark SQL application"""



from pyspark.sql import SparkSession

from pyspark.sql.functions import col



spark = SparkSession.builder \

    .appName("Job2 spark sql") \

    .getOrCreate()



# Carica il dataset CSV

data = spark.read.csv("file:///home/martina/Scaricati/PrimoProgettoBigData/dataset_clean.csv", header=True, inferSchema=True)



# Rinomina le colonne con il formato _c$numerocolonna

data = data.toDF("_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7")



# Calcola l'utilità delle recensioni

data = data.withColumn("Utility", col("_c3") / col("_c4"))



# Calcola l'apprezzamento medio per ogni utente

user_appreciation = data.groupBy("_c2") \

    .agg({"Utility": "avg"}) \

    .withColumnRenamed("avg(Utility)", "AverageAppreciation")



# Ordina gli utenti in base all'apprezzamento medio e al nome utente

sorted_users = user_appreciation.orderBy(["AverageAppreciation", "_c2"], ascending=[False, True])



# Salva la lista ordinata degli utenti e il loro apprezzamento in un file CSV

sorted_users.coalesce(1).write.csv("file:///home/martina/Scaricati/PrimoProgettoBigData/Job2/sparksql/output", mode="overwrite")
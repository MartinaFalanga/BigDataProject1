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


# Crea una vista temporanea per eseguire query SQL

data.createOrReplaceTempView("reviews")



# Calcola l'apprezzamento medio per ogni utente e ordina gli utenti in base all'apprezzamento medio e al nome utente

sorted_users = spark.sql("""

    SELECT _c2 as UserId, AVG(Utility) as AverageAppreciation

    FROM reviews

    GROUP BY _c2

    ORDER BY AverageAppreciation DESC, _c2 ASC

""")




# Salva la lista ordinata degli utenti e il loro apprezzamento in un file CSV

sorted_users.coalesce(1).write.csv("file:///home/martina/Scaricati/PrimoProgettoBigData/Job2/sparksql/output", mode="overwrite")

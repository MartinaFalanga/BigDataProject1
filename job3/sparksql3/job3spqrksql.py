#!/usr/bin/env python3

"""spark SQL application"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws



spark = SparkSession.builder \

    .appName("Job3 spark sql") \

    .getOrCreate()



# Leggi il dataset CSV e crea un DataFrame

df = spark.read.csv("file:///home/martina/Scaricati/PrimoProgettoBigData/dataset_clean.csv", header=False, inferSchema=True)



# Assegna i nomi

df = df.select(

    col("_c0").alias("Id"),

    col("_c1").alias("ProductId"),

    col("_c2").alias("UserId"),

    col("_c3").alias("HelpfulnessNumerator"),

    col("_c4").alias("HelpfulnessDenominator"),

    col("_c5").alias("Score"),

    col("_c6").alias("Time"),

    col("_c7").alias("Text")

)



# Filtra le righe in base al punteggio (Score >= 4)

df_filtered = df.filter(df.Score >= 4)



# Crea una vista temporanea per eseguire query SQL

df_filtered.createOrReplaceTempView("reviews")



# Esegui un join su UserId e ProductId, raggruppa gli utenti e conta i prodotti in comune

# Filtra i gruppi in base al conteggio dei prodotti in comune (>= 3)

# Ordina i risultati in base allo UserId del primo elemento del gruppo

# Rimuovi i duplicati

query = """

SELECT r1.UserId AS User1, r2.UserId AS User2, COLLECT_LIST(r1.ProductId) AS SharedProducts

FROM reviews r1

JOIN reviews r2 ON r1.ProductId = r2.ProductId AND r1.UserId < r2.UserId

GROUP BY r1.UserId, r2.UserId

HAVING COUNT(*) >= 3

ORDER BY r1.UserId

"""



result = spark.sql(query)



# Converte la colonna SharedProducts in una stringa

result = result.withColumn("SharedProducts", concat_ws(",", "SharedProducts"))

#Output su file
result.write.csv("file:///home/martina/Scaricati/PrimoProgettoBigData/Job3/sparksql/output", header=False)
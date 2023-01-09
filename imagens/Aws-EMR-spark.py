# Importação das bibliotecas necessárias para o processamento

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# configuração da sessão Spark
spark = SparkSession \
    .builder \
    .appName("job-emr-spark") \
    .getOrCreate()

# definindo o método de logs da aplicação como INFO indicada em ambiente DEV opçoes[INFO,ERROR]
spark.sparkContext.setLogLevel("ERROR")

# Carregando os dados do nosso Data Lake
df = spark.read.format("csv")\
    .option("header", "True")\
    .option("inferSchema","True")\
    .csv("s3a://landing-jaime/*.csv")



# imprime os dados lidos da LANDING
print ("\nImprime os dados lidos da lading:")
print (df.show())

# imprime o schema do dataframe
print ("\nImprime o schema do dataframe lido da landing:")
print (df.printSchema())

#Limpando os dados
df.na.fill(value=0,subset=["quantity"]).show()

# converte para formato parquet
print ("\nEscrevendo os dados lidos da raw para parquet na processing zone...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("s3a://processing-jaime/df-parquet-file.parquet")

# lendo arquivos parquet
df_parquet = spark.read.format("parquet")\
 .load("s3a://processing-jaime/df-parquet-file.parquet")

# imprime os dados lidos em parquet
print ("\nImprime os dados lidos em parquet da processing zone")
print (df_parquet.show())

# cria uma view para trabalhar com sql
df_parquet.createOrReplaceTempView("Dados_Sql")

# processa os dados conforme regra de negócio
df_sql  = spark.sql("SELECT BNF_CODE as Bnf_code \
                       ,SUM(ACT_COST) as Soma_cost \
                       ,SUM(QUANTITY) as Soma_Quantity \
                       ,SUM(ITEMS) as Soma_items \
                       ,AVG(ACT_COST) as Media_cost \
                      FROM Dados_Sql \
                      GROUP BY bnf_code")


# converte para formato parquet
print ("\nEscrevendo os dados processados na Curated Zone...")

# converte os dados processados para parquet e escreve na curated zone
df_sql.write.format("parquet")\
         .mode("overwrite")\
         .save("s3a://cureted-jaime/df-result-file.parquet")

# para a aplicação
spark.stop()
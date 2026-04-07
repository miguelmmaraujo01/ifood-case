from pyspark.sql.functions import col, hour, month
from pyspark.sql import SparkSession, DataFrame
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_silver(spark: SparkSession, ENV: str) -> DataFrame:
    #camada silver

    bronze_table = "workspace.taxi.bronze_taxi"
    silver_table = "workspace.taxi.silver_taxi"
    df_bronze = spark.read.table(bronze_table)

    valid_columns = [
        "vendorid",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "year",
        "month"
    ]

    try:
        logger.info("Validando colunas necessárias camada Silver")
        df_silver = df_bronze.select(*valid_columns)
    except Exception as e:
        logger.error(f"Erro ao selecionar colunas necessárias: {e}")
        #print(f"Erro ao selecionar colunas necessárias: {e}")
        raise e

    # Padronizacao da tipagem e colunas para facilitar o processamento
    logger.info("Equalizando tipagem colunas camada Silver")
    df_silver = df_silver \
        .withColumn("passenger_count", col("passenger_count").cast("int")) \
        .withColumn("total_amount", col("total_amount").cast("double")) \
        .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
        .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp")) \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("month", col("month").cast("int")) 

    logger.info("Salvando camada Silver")


    try:
        df_silver = df_silver.repartition(4)

        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .saveAsTable(silver_table)

    except Exception as e:
        logger.error(f"Erro ao criar Camada Silver: {e}")
        raise

    logger.info("Camada Silver criada com sucesso")

    return df_silver
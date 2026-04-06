from pyspark.sql.functions import col, hour, month
from pyspark.sql import DataFrame


def run_silver(df_bronze: DataFrame) -> DataFrame:
#camada silver

    valid_columns = [
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "year",
        "month"
    ]

    try:
        df_silver = df_bronze.select(*valid_columns)
    except Exception as e:
        print(f"Erro ao selecionar colunas necessárias: {e}")
        raise


    # Utilizacao apenas das colunas necessária, reduzindo o volume de dados e processamento.  
    df_silver = df_bronze.select(
        col("VendorID"),
        col("passenger_count"),
        col("total_amount"),
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),
        col("year"),
        col("month")
    )

    # Padronizacao da tipagem e colunas para facilitar o processamento
    df_silver = df_silver \
        .withColumn("passenger_count", col("passenger_count").cast("int")) \
        .withColumn("total_amount", col("total_amount").cast("double")) \
        .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
        .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp")) \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("month", col("month").cast("int")) 

    #display(df_silver)
    #df_silver.createOrReplaceTempView("silver_taxi")

    return df_silver
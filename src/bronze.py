from pyspark.sql.functions import lit, current_timestamp, date_trunc, col
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_bronze(spark: SparkSession, ENV: str):

    dict_months = ["2023-01","2023-02","2023-03","2023-04","2023-05"]

    # tabela delta (correto no databricks)
    table_name = "workspace.taxi.bronze_taxi"

    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.taxi")

    for m in dict_months:
        logger.info(f"Carregando mês: {m}")

        if ENV == "databricks":
            path = f"/Volumes/workspace/taxi/file_taxi/yellow_tripdata_{m}.parquet"
        else:
            # só pra local (opcional)
            path = f"./Volumes/workspace/taxi/file_taxi/yellow_tripdata_{m}.parquet"

        try:
            df = spark.read.parquet(path)
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo: {e}")
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

        # Padronizacao nome colunas para minusculo
        new_columns = []
        for col_name in df.columns:
            new_columns.append(col_name.lower())
        df = df.toDF(*new_columns)

        df = df \
            .withColumn("vendorid", col("vendorid").cast("long")) \
            .withColumn("passenger_count", col("passenger_count").cast("double")) \
            .withColumn("ratecodeid", col("ratecodeid").cast("double")) \
            .withColumn("pulocationid", col("pulocationid").cast("long")) \
            .withColumn("dolocationid", col("dolocationid").cast("long")) \
            .withColumn("total_amount", col("total_amount").cast("double"))

        year, month = m.split("-")

        df = df.withColumn("year", lit(year)) \
               .withColumn("month", lit(month)) \
               .withColumn("dat_import", date_trunc("second", current_timestamp()))

        # 🔥 grava corretamente como tabela delta
        df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year", "month") \
            .saveAsTable(table_name)

        logger.info(f"Dados gravados na tabela: {table_name}")

    logger.info("Bronze finalizada com sucesso")
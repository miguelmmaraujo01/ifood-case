from pyspark.sql.functions import lit, current_timestamp, date_trunc, col
from pyspark.sql import SparkSession
import logging
import os
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_bronze(spark: SparkSession, ENV: str, mode_load: str, list_year_months: list = None):


    # tabela delta para gravar dados bronze
    table_name = "workspace.taxi.bronze_taxi"

    logger.info(f"Executando Bronze - {ENV} - {mode_load}")

    if mode_load == 'full' or mode_load is None:
        list_year_months = None
        spark.sql("DROP TABLE IF EXISTS workspace.taxi.bronze_taxi")
        spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.taxi")

        #glob carrega os arquivos em dicionario
        files = glob.glob("/Volumes/workspace/taxi/file_taxi/yellow_tripdata_*.parquet")

        for f in files:
            file_name = f.split("/")[-1]
            if "yellow_tripdata_" in file_name and file_name.endswith(".parquet"):
                file_part_year_month = file_name.replace("yellow_tripdata_", "").replace(".parquet", "")
                # y, m = file_part_year_month.split("-")
                logger.info(f"Carregando arquivo: {f}")
                try:
                    df = spark.read.parquet(f)
                except Exception as e:
                    logger.error(f"Erro ao carregar arquivo: {e}")
                    raise e

                # Padronizacao nome colunas para minusculo e cast garantindo a estrutura dos dados.
                df = transformation_estructural_bronze(df, file_part_year_month)

                # gravando dados bronze
                df = df.repartition(4)
                df.write \
                    .format("delta") \
                    .mode("append") \
                    .partitionBy("year", "month") \
                    .saveAsTable(table_name)

                logger.info(f"Dados gravados na tabela: {table_name}")

        logger.info("Bronze finalizada com sucesso")
    # incremental 
    else:
        ###spark.sql("DROP TABLE workspace.taxi.silver_taxi")
        
        dfs = []
        spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.taxi")

        if list_year_months is None:
            logger.error("list_year_months necessário para carregamento incremental")
            raise ValueError("list_year_months necessário para carregamento incremental")

        for m in list_year_months:

            logger.info(f"Carregando ano mês: {m}")

            if ENV == "databricks":
                path = f"/Volumes/workspace/taxi/file_taxi/yellow_tripdata_{m}.parquet"
            else:
                # só pra local (opcional)
                path = f"./Volumes/workspace/taxi/file_taxi/yellow_tripdata_{m}.parquet"

            try:
                df = spark.read.parquet(path)
            except Exception as e:
                logger.error(f"Erro ao carregar arquivo: {e}")
                raise FileNotFoundError(f"Arquivo não encontrado: {path}") from e

            # Padronizacao nome colunas para minusculo e cast garantindo a estrutura dos dados.
            df = transformation_estructural_bronze(df, m)
            dfs.append(df)

        # junta tudo de forma simples
        df_final = dfs[0]
        for df in dfs[1:]:
            df_final = df_final.unionByName(df)

        list_append_year_month = []
        for ym  in list_year_months:
            year, month = ym.split("-")
            list_append_year_month.append(f"(year = '{year}' AND month = '{month}')")
        replaceYearMonth = " OR ".join(list_append_year_month)
        logger.info(f"replaceYearMonth : {replaceYearMonth}")

        # gravando dados bronze #df.write \
        df_final = df_final.repartition(4)
        df_final.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", replaceYearMonth) \
            .partitionBy("year", "month") \
            .saveAsTable(table_name)

        logger.info(f"Dados gravados na tabela: {table_name}")

        logger.info("Bronze finalizada com sucesso")


def transformation_estructural_bronze(df, file_part_year_month):
        m = file_part_year_month
        logger.info("DataQuality - Estrutural camada Bronze")
        # Colunas necessárias
        # df = df \
        #     .withColumn("vendorid", col("vendorid").cast("long")) \
        #     .withColumn("passenger_count", col("passenger_count").cast("double")) \
        #     .withColumn("ratecodeid", col("ratecodeid").cast("double")) \
        #     .withColumn("pulocationid", col("pulocationid").cast("long")) \
        #     .withColumn("dolocationid", col("dolocationid").cast("long")) \
        #     .withColumn("total_amount", col("total_amount").cast("double"))

        #https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
        df = df \
            .withColumn("vendorid", col("vendorid").cast("long")) \
            .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
            .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp")) \
            .withColumn("passenger_count", col("passenger_count").cast("double")) \
            .withColumn("trip_distance", col("trip_distance").cast("double")) \
            .withColumn("ratecodeid", col("ratecodeid").cast("double")) \
            .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast("string")) \
            .withColumn("pulocationid", col("pulocationid").cast("long")) \
            .withColumn("dolocationid", col("dolocationid").cast("long")) \
            .withColumn("payment_type", col("payment_type").cast("int")) \
            .withColumn("fare_amount", col("fare_amount").cast("double")) \
            .withColumn("extra", col("extra").cast("double")) \
            .withColumn("mta_tax", col("mta_tax").cast("double")) \
            .withColumn("tip_amount", col("tip_amount").cast("double")) \
            .withColumn("tolls_amount", col("tolls_amount").cast("double")) \
            .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double")) \
            .withColumn("total_amount", col("total_amount").cast("double")) \
            .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double")) \
            .withColumn("airport_fee", col("airport_fee").cast("double"))
            #.withColumn("cbd_congestion_fee", col("cbd_congestion_fee").cast("double")) #Congestion Relief Zone starting Jan. 5, 2025. Só existe apartir de 2025


        # criando particionamento rastreabilidade para governanca
        year, month = m.split("-")
        df = df.withColumn("year", lit(year)) \
               .withColumn("month", lit(month)) \
               .withColumn("dat_import", date_trunc("second", current_timestamp()))

        new_columns = []
        for col_name in df.columns:
            new_columns.append(col_name.lower())
        df = df.toDF(*new_columns)

        return df
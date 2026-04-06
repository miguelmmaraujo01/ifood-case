from pyspark.sql import SparkSession
from bronze import run_bronze
from silver import run_silver
from gold import gold_avg_amount_by_month, gold_avg_passengers_by_hour
import os


from utils.log import setup_logger

logger = setup_logger()

ENV = os.getenv("ENV", "local") ## ou  databricks

def main():
    spark = SparkSession.builder.appName("ifood-case").getOrCreate()
    logger.info(f"Ambiente de execução: {ENV}")

    # Bronze
    logger.info("Executando Camada Bronze")
    df_bronze = run_bronze(spark, ENV)

    # Silver
    logger.info("Executando Camada Silver")
    df_silver = run_silver(df_bronze)

    # Criando view para análises
    df_silver.createOrReplaceTempView("silver_taxi")

    logger.info("View Spark silver_taxi finalizada com sucesso!")

    logger.info("Executando Camada Gold - Análise de valor médio por mês")

    df_gold_month = gold_avg_amount_by_month(df_silver)
    df_gold_month.show()
    df_gold_month.createOrReplaceTempView("gold_avg_amount_by_month")


    logger.info("Executando Camada Gold - Análise de passageiros por hora")

    df_gold_hour = gold_avg_passengers_by_hour(df_silver)
    df_gold_hour.show()
    df_gold_hour.createOrReplaceTempView("gold_avg_passengers_by_hour")



if __name__ == "__main__":
    main()
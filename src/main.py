from pyspark.sql import SparkSession
from bronze import run_bronze
from silver import run_silver
from gold import gold_create_schema_view 

from datetime import datetime
import time

from utils.log import setup_logger

logger = setup_logger()

ENV = "databricks"

def main():
    start = datetime.now()
    logger.info(f"Execução iniciada: {start}")
    spark = SparkSession.builder.appName("ifood-case").getOrCreate()

    logger.info(f"Ambiente de execução: {ENV}")
    logger.info(f"Ambiente de execução: {ENV}")

    # Bronze
    logger.info("Executando Camada Bronze")
    run_bronze(spark, ENV)

    # Silver
    logger.info("Executando Camada Silver")
    run_silver(spark, ENV)

    # # Gold
    logger.info("Executando Camada Gold")
    gold_create_schema_view(spark, ENV)

    logger.info("Validando views camada Gold")
    spark.sql("SELECT * FROM workspace.taxi.gold_media_valor_total_mes").show()
    spark.sql("SELECT * FROM workspace.taxi.gold_media_passageiros_hora").show()

    end = datetime.now()
    logger.info(f"Execução finalizada: {end}")
    logger.info(f"Tempo de execução: {end-start}")

if __name__ == "__main__":
    main()
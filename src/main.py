from pyspark.sql import SparkSession
from bronze import run_bronze
from silver import run_silver
from gold import gold_create_schema_view 
from datetime import datetime
import time
from utils.log import setup_logger

logger = setup_logger()

# mode_load = 'full' (carrega todos os dados do unit catalog) 
# OU 
# mode_load = 'incremental' (Carrega apenas os dados que estão passados no dict_month manualmente, podendo servir como reprocessamento)

ENV = "databricks"
mode_load = 'incremental'#'incremental' #full
list_year_month = ["2023-01", "2023-05", "2023-06"] #None

def main():
    start = datetime.now()
    logger.info(f"Execução iniciada: {start}")
    spark = SparkSession.builder.appName("ifood-case").getOrCreate()
    
    logger.info(f"Ambiente de execução: {ENV}")

    # Bronze
    logger.info("Chamando Camada Bronze")
    run_bronze(spark, ENV, mode_load, list_year_month)

    # Silver
    logger.info("Chamando Camada Silver")
    run_silver(spark, ENV)

    # # Gold
    logger.info("Chamando Camada Gold")
    gold_create_schema_view(spark, ENV)

    logger.info("Validando views camada Gold")
    spark.sql("SELECT * FROM workspace.taxi.gold_media_valor_total_mes").show()
    spark.sql("SELECT * FROM workspace.taxi.gold_media_passageiros_hora").show()

    end = datetime.now()
    logger.info(f"Execução finalizada: {end}")
    logger.info(f"Tempo de execução: {end-start}")

if __name__ == "__main__":
    main()
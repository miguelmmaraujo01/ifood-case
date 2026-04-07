from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def gold_create_schema_view(spark: SparkSession, ENV: str):

    schema = "workspace.taxi"

    # garante schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS " + schema)

    logger.info("Criando views da camada GOLD")

    # -------------------------
    # REGRA 1
    # Qual a média de valor total (total\_amount) recebido em um mês considerando todos os yellow táxis da frota?
    # Média de valor total por mês
    # -------------------------
    spark.sql("""
        CREATE OR REPLACE VIEW """ + schema + """.gold_media_valor_total_mes AS
        SELECT 
            month AS des_mes,
            AVG(total_amount) AS val_media_total_mes
        FROM """ + schema + """.silver_taxi
        GROUP BY month
        ORDER BY des_mes ASC
    """)
    # -------------------------
    # REGRA 2
    # Qual a média de passageiros (passenger\_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?
    # Média de passageiros por hora (mês de maio)
    # -------------------------
    spark.sql("""
        CREATE OR REPLACE VIEW """ + schema + """.gold_media_passageiros_hora AS
        SELECT 
            HOUR(tpep_pickup_datetime) AS num_horas,
            CASE WHEN HOUR(tpep_pickup_datetime) < 12 THEN 'AM' ELSE 'PM' END AS des_am_pm,
            AVG(passenger_count) AS val_media_passageiros_hora
        FROM """ + schema + """.silver_taxi
        WHERE month = '05'
        GROUP BY HOUR(tpep_pickup_datetime)
        ORDER BY num_horas ASC
    """)

    logger.info("Views Camada Gold criadas com sucesso")
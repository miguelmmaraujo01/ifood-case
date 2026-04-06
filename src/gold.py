from pyspark.sql.functions import hour, avg
from pyspark.sql import DataFrame

# %sql
# --GOLD
# --2 - Qual a média de valor total total\_amount recebido em um mês considerando todos os yellow táxis da frota?

def gold_avg_amount_by_month(df_silver: DataFrame) -> DataFrame:

  df_gold_month = df_silver.groupBy("month") \
      .agg(avg("total_amount").alias("avg_total_amount")) \
      .orderBy("month")

  #display(df_gold_month)
  return df_gold_month


# %sql
# -- 3 Qual a média de passageiros passenger\_count por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

def gold_avg_passengers_by_hour(df_silver: DataFrame) -> DataFrame :
  df_gold_hour = df_silver \
      .filter(df_silver.month == 5) \
      .groupBy(hour("tpep_pickup_datetime").alias("pickup_hour")) \
      .agg(avg("passenger_count").alias("avg_passengers")) \
      .orderBy("pickup_hour")

  #display(df_gold_hour)
  return df_gold_hour
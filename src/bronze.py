from pyspark.sql.functions import lit, current_timestamp, date_trunc
from pyspark.sql import SparkSession, DataFrame

def run_bronze(spark: SparkSession, ENV: str) -> DataFrame:
    #camada bronze

    #validar catalog
    #spark.read.parquet("/Volumes/workspace/taxi/file_taxi/yellow_tripdata_2023-01.parquet").show(5)

    # Meses para utilizacao (processamento incremental)
    dict_months = ["2023-01","2023-02","2023-03","2023-04","2023-05"]

    df_bronze = None

    # Iteracao para carregar os dados por mes
    for m in dict_months:
        print(f"Carregando mês: {m}")

        #Conforme recomentado uso databrikcks
        #Carga via Upload File - Leitura dos dados via Unity Catalog Volume (camada landing zone), simulando Data Lake.
        if ENV == "databricks":
            path = f"/Volumes/workspace/taxi/file_taxi/yellow_tripdata_{m}.parquet"
        #local teste
        else: 
            path = f"../Volumes/workspace/taxi/file_taxi/yellow_tripdata_{m}.parquet"

        try:
            df = spark.read.parquet(path)
        except Exception as e:
            print(f"Erro ao carregar arquivo: {e}")
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")
            
        # Criando colunas de particionamento para facilitar o processamento incremental , para melhor gestao de dados
        year, month = m.split("-")
        df = df.withColumn("year", lit(year)) \
            .withColumn("month", lit(month)) \
                .withColumn("dat_import", date_trunc("second", current_timestamp())) 

        # Unificacao dos meses em um único dataframe
        if df_bronze is None:
            df_bronze = df
        else:
            df_bronze = df_bronze.unionByName(df)

    # validando dados lidos 
    #df_bronze.groupBy("year", "month").count().show()
    # +----+-----+-------+
    # |year|month|  count|
    # +----+-----+-------+
    # |2023|   01|3066766|
    # |2023|   02|2913955|
    # |2023|   03|3403766|
    # |2023|   04|3288250|
    # |2023|   05|3513649|
    # +----+-----+-------+
    #display(df_bronze)


    return df_bronze
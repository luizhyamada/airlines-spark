import os
import re
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, split, size, when, current_timestamp

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ["PATH"]

builder = SparkSession.builder \
    .appName("Lakehouse Silver Layer") \
    .master("local[1]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

current_dir = os.path.dirname(os.path.abspath(__file__))
bronze_path = os.path.join(current_dir, "01-bronze")
silver_path = os.path.join(current_dir, "02-silver")

os.makedirs(silver_path, exist_ok=True)

bronze_tables = [
    "air_cia", "vra"
]

def transform_air_cia(df):
    return df.select(
        col("razao_social").alias("company_name"),
        split(col("icao_iata"), " ").alias("icao_iata"),
        col("cnpj").alias("cnpj"),
        col("atividades_aereas").alias("air_activities"),
        col("endereco_sede").alias("headquarters_address"),
        col("telefone").alias("phone"),
        col("email").alias("email"),
        col("decisao_operacional").alias("operational_decision"),
        col("data_decisao_operacional").alias("operational_decision_date"),
        col("validade_operacional").alias("operational_validity")
    ).withColumn(
        "icao", col("icao_iata").getItem(0)
    ).withColumn(
        "iata", when(size(col("icao_iata")) > 1, col("icao_iata").getItem(1)).otherwise(None)
    ).drop("icao_iata")


def transform_vra(df):
    return df.select(
        col("ChegadaPrevista").alias("estimated_arrival"),
        col("ChegadaReal").alias("actual_arrival"),
        col("CódigoAutorização").alias("authorization_code"),
        col("CódigoJustificativa").alias("justification_code"),
        col("CódigoTipoLinha").alias("line_type_code"),
        col("ICAOAeródromoDestino").alias("icao_destination_aerodrome"),
        col("ICAOAeródromoOrigem").alias("icao_origin_aerodrome"),
        col("ICAOEmpresaAérea").alias("icao_airline"),
        col("NúmeroVoo").alias("flight_number"),
        col("PartidaPrevista").alias("estimated_departure"),
        col("PartidaReal").alias("actual_departure"),
        col("SituaçãoVoo").alias("flight_status")
    )

transformations = {
    "air_cia": transform_air_cia,
    "vra" : transform_vra
}

for table in bronze_tables:
    input_path = os.path.join(bronze_path, table)
    output_path = os.path.join(silver_path, table)

    if not os.path.exists(input_path):
        print(f"Warning: Bronze table not found: {input_path}. Skipping...")
        continue

    try:
        df = spark.read.format("delta").load(input_path)
        print(f"Loaded Bronze Table for {table}")

        if table in transformations:
            df_transformed = transformations[table](df)
            df_transformed = df_transformed.withColumn("processed_at", current_timestamp())

            df_transformed.write.format("delta").mode("overwrite").save(output_path)
            print(f"Silver table for {table} written to {output_path}")

        else:
            print(f"No transformation defined for {table}. Skipping...")

    except Exception as e:
        print(f"Error processing {table}: {e}")

print("Silver layer populated successfully.")
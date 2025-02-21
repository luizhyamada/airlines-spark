import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ["PATH"]

builder = SparkSession.builder \
    .appName("") \
    .master("local[1]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

current_dir = os.path.dirname(os.path.abspath(__file__))
base_input_air_cia_path = os.path.abspath(os.path.join(current_dir, "../datasets/AIR_CIA"))
base_output_path = os.path.join(current_dir, "01-bronze")

os.makedirs(base_output_path, exist_ok=True)

def process_files(input_base_path, output_path, file_format):
    df = spark.read.option("delimiter", ";").option("header", "True").csv(os.path.join(input_base_path, '*.csv'))
    df = df.withColumnRenamed("Razão Social", "razao_social") \
        .withColumnRenamed("ICAO IATA", "icao_iata") \
        .withColumnRenamed("CNPJ", "cnpj") \
        .withColumnRenamed("Atividades Aéreas", "atividades_aereas") \
        .withColumnRenamed("Endereço Sede", "endereco_sede") \
        .withColumnRenamed("Telefone", "telefone") \
        .withColumnRenamed("E-Mail", "email") \
        .withColumnRenamed("Decisão Operacional", "decisao_operacional") \
        .withColumnRenamed("Data Decisão Operacional", "data_decisao_operacional") \
        .withColumnRenamed("Validade Operacional", "validade_operacional")

    df.write.format("delta").mode("overwrite").save(output_path)
    print(f"Combined Delta table for {file_format.upper()} written to {output_path}")

air_cia_output_path = os.path.join(base_output_path, "air_cia")

process_files(base_input_air_cia_path, air_cia_output_path, file_format='csv')

print("Bronze layer populated successfully.")
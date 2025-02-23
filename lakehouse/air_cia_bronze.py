import os
from config import base_input_air_cia_path, bronze_air_cia_output_path
from spark_session import get_spark_session

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ["PATH"]

spark = get_spark_session()

os.makedirs(bronze_air_cia_output_path, exist_ok=True)

spark.read.option("delimiter", ";").option("header", "True").csv \
    (os.path.join(base_input_air_cia_path, '*.csv')) \
    .withColumnRenamed("Razão Social", "razao_social") \
    .withColumnRenamed("ICAO IATA", "icao_iata") \
    .withColumnRenamed("CNPJ", "cnpj") \
    .withColumnRenamed("Atividades Aéreas", "atividades_aereas") \
    .withColumnRenamed("Endereço Sede", "endereco_sede") \
    .withColumnRenamed("Telefone", "telefone") \
    .withColumnRenamed("E-Mail", "email") \
    .withColumnRenamed("Decisão Operacional", "decisao_operacional") \
    .withColumnRenamed("Data Decisão Operacional", "data_decisao_operacional") \
    .withColumnRenamed("Validade Operacional", "validade_operacional") \
    .write.format("delta").mode("overwrite").save(bronze_air_cia_output_path)

print(f"Bronze layer populated successfully. Delta table written to {bronze_air_cia_output_path}")
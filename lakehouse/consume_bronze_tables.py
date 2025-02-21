import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ["PATH"]

builder = SparkSession.builder \
    .appName("Read Bronze Tables") \
    .master("local[1]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

current_dir = os.path.dirname(os.path.abspath(__file__))
bronze_path = os.path.join(current_dir, "01-bronze")

bronze_tables = [
    "air_cia", "vra"
]

for table in bronze_tables:
    path = os.path.join(bronze_path, table)
    if os.path.exists(path):
        print(f"\nSchema of Bronze table: {table}")
        try:
            df = spark.read.format("delta").load(path)
            df.printSchema()
            print(f"\nContents of Bronze table: {table}")
            df.show(truncate=False)
        except Exception as e:
            print(f"Error reading Bronze table {table}: {e}")
    else:
        print(f"Bronze table {table} does not exist at path: {path}")
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ["PATH"]

builder = SparkSession.builder \
    .appName("Read Silver Tables") \
    .master("local[1]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

current_dir = os.path.dirname(os.path.abspath(__file__))
silver_path = os.path.join(current_dir, "02-silver")

silver_tables = [
    "air_cia", "vra"
]

for table in silver_tables:
    path = os.path.join(silver_path, table)
    if os.path.exists(path):
        print(f"\nSchema of Silver table: {table}")
        try:
            df = spark.read.format("delta").load(path)
            df.printSchema()
            print(f"\nContents of Silver table: {table}")
            df.show(truncate=False)
        except Exception as e:
            print(f"Error reading Silver table {table}: {e}")
    else:
        print(f"Silver table {table} does not exist at path: {path}")
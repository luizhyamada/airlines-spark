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
base_input_vra_path = os.path.abspath(os.path.join(current_dir, "../datasets/VRA"))
base_output_path = os.path.join(current_dir, "01-bronze/vra")

os.makedirs(base_output_path, exist_ok=True)

df = spark.read.json(os.path.join(base_input_vra_path, '*.json'))

df.write.format("delta").mode("overwrite").save(base_output_path)

print("Bronze layer populated successfully.")
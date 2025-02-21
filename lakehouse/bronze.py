import os
from pyspark.sql import SparkSession, DataFrame
from delta import configure_spark_with_delta_pip
from functools import reduce

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
base_input_vra_path = os.path.abspath(os.path.join(current_dir, "../datasets/VRA"))
base_output_path = os.path.join(current_dir, "01-bronze")

os.makedirs(base_output_path, exist_ok=True)

def process_air_cia_files(input_base_path, output_path):
    df_list = []
    for file_name in os.listdir(input_base_path):
        input_path = os.path.join(input_base_path, file_name)
        if file_name.endswith('.csv'):
            if os.path.exists(input_path):
                try:
                    df = spark.read.option("multiline", "True").csv(input_path)
                    df_list.append(df)
                    print(f"Loaded {df.count()} rows from dataset: {file_name}")
                except Exception as e:
                    print(f"Error processing {input_path}: {e}")
            else:
                print(f"Warning: Dataset file not found: {input_path}. Skipping...")
    
    if df_list:
        combined_df = reduce(DataFrame.union, df_list)
        combined_df.write.format("delta").mode("overwrite").save(output_path)
        print(f"Combined Delta table for AIR_CIA written to {output_path}")

def process_files(input_base_path, output_path, file_format):
    df_list = []
    for file_name in os.listdir(input_base_path):
        input_path = os.path.join(input_base_path, file_name)
        if (file_format == 'csv' and file_name.endswith('.csv')) or (file_format == 'json' and file_name.endswith('.json')):
            if os.path.exists(input_path):
                try:
                    if file_format == 'csv':
                        df = spark.read.option("multiline", "True").option("delimiter", ";").csv(input_path)
                    elif file_format == 'json':
                        df = spark.read.json(input_path)

                    df_list.append(df)
                    print(f"Loaded {df.count()} rows from dataset: {file_name}")
                except Exception as e:
                    print(f"Error processing {input_path}: {e}")
            else:
                print(f"Warning: Dataset file not found: {input_path}. Skipping...")
    
    if df_list:
        combined_df = reduce(DataFrame.union, df_list)
        combined_df.write.format("delta").mode("overwrite").save(output_path)
        print(f"Combined Delta table for {file_format.upper()} written to {output_path}")

air_cia_output_path = os.path.join(base_output_path, "air_cia")
vra_output_path = os.path.join(base_output_path, "vra")

process_files(base_input_air_cia_path, air_cia_output_path, file_format='csv')
process_files(base_input_vra_path, vra_output_path, file_format='json')

print("Bronze layer populated successfully.")
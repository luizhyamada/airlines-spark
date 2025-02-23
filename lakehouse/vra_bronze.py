import os
from spark_session import get_spark_session
from config import base_input_vra_path, bronze_vra_output_path

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ["PATH"]

spark = get_spark_session()

os.makedirs(bronze_vra_output_path, exist_ok=True)

spark.read.json(os.path.join(base_input_vra_path, '*.json')) \
    .write.format("delta").mode("overwrite").save(bronze_vra_output_path)

print("Bronze layer populated successfully.")
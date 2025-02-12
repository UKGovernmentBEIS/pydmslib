import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def read_parquet_files_and_add_input_file(base_dir, pattern=""):
    """
    Reads Parquet files from the specified directory and adds a new column indicating the source file name.
    Args:
        base_dir (str): The base directory where the Parquet files are stored.
        pattern (str, optional): An optional pattern to filter specific files. Defaults to an empty string.
    Returns:
        DataFrame: A PySpark DataFrame with an additional column 'inputFile' containing the source file name.
    """
    file_path = base_dir + pattern
    df = spark.read.parquet(file_path)
    return df.withColumn("inputFile", f.col("_metadata.file_name"))

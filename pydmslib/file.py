""" file.py contains methods that help with standardising reading/cleaning steps """
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def read_parquet_files_and_add_input_file(directory: str, pattern: str = ""):
    """
    Reads Parquet files from the specified directory and adds a new column indicating the source file name.
    Args:
        dir (str): The directory where the Parquet files are stored. For reading pyspark dataframes, you must include the LAKE_ROOT_SPARK variable in the dir variable.
        pattern (str, optional): An optional pattern to filter specific files. Defaults to an empty string.
    Returns:
        DataFrame: A PySpark DataFrame with an additional column 'inputFile' containing the source file name.
    """

    file_path = directory + pattern
    df = spark.read.parquet(file_path)
    return df.withColumn("inputFile", f.col("_metadata.file_name"))

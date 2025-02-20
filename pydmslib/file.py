""" file.py contains methods that help with standardising reading/cleaning steps """
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.dbutils import DBUtils


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
def read_parquet_files_and_add_input_file(directory: str, pattern: str = ""):
    """
    Reads Parquet files from the specified directory and 
    adds a new column indicating the source file name.
    Args:
        dir (str): The directory where the Parquet files are stored. 
        For reading pyspark dataframes, you must include 
        the LAKE_ROOT_SPARK variable in the dir variable.
        pattern (str, optional): An optional pattern to filter specific files. 
        Defaults to an empty string.
    Returns:
        DataFrame: A PySpark DataFrame with an 
        additional column 'inputFile' containing the source file name.
    """

    file_path = directory + pattern
    df = spark.read.parquet(file_path)
    return df.withColumn("inputFile", f.col("_metadata.file_name"))

def exists(path):
    """

    :param path: hdfs path
    :return: If the supplied path exists (bool)
    """
    # https://forums.databricks.com/questions/20129/how-to-check-file-exists-in-databricks.html
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

def crawl(path: str, trim_size=None):
    """
    Lists all files under a directory
    :param path: hdfs path
    :param trim_size: How many characters to trim returned paths by (defaults to path length)
    :return: yields strings
    """
    if not path.endswith("/"):
        path += "/"
    
    if trim_size is None:
        if path.startswith("dbfs:/"):
            trim_size = len(path)
        elif path.startswith("/"):
            trim_size = 5 + len(path)
        else:
            trim_size = 6 + len(path)
    
    def list_files(directory):
        for f in dbutils.fs.ls(directory):
            if f.path.endswith("/"):
                yield from list_files(f.path)
            else:
                yield f.path[trim_size:]
    
    return list_files(path)

def identify_to_process(metadata, process_dir, my_config=""):
    # Calculate files already processed
    if metadata is None:
        already_processed = None
    else:
        if "config" not in metadata.columns:
            metadata = metadata.withColumn("config", f.lit(my_config))
            
        already_processed = metadata.select("file_name", "config").distinct() 

    # Check if grant directory exists
    if not exists(process_dir):
        if metadata is None:
            # Assume just not created yet and return empty
            return []
        else:
            # This could be a genuine issue, raise exception
            raise Exception("To Process Directory Not Found")

    # Get all files in the grant directory
    all_grant_files = list(crawl(process_dir))

    # Add config to all_grant_files
    if already_processed is None:
        files_to_process = [f for f, _ in all_grant_files] # Just return all files if no metadata
    else:
        # Create a DataFrame for all_grant_files

        schema = StructType([
            StructField("file_name", StringType(), True),  # True means nullable
            StructField("config", StringType(), True)
        ])
        
        all_files_df = spark.createDataFrame(all_grant_files, ["file_name", "config"], schema=schema) # Requires a spark session

        # Use a left anti join to efficiently find the difference
        files_to_process_df = all_files_df.join(already_processed, ["file_name", "config"], "left_anti")

        # Extract the file names
        files_to_process = [row.file_name for row in files_to_process_df.collect()]

    # Done
    return files_to_process


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
    """Lists all files under a directory, optimized."""

    if not path.endswith("/"):
        path += "/"

    if trim_size is None:
        trim_size = len(path) if path.startswith("dbfs:/") else (5 + len(path) if path.startswith("/") else 6 + len(path))

    try:  # Handle potential exceptions during file listing
        files = dbutils.fs.ls(path)  # Initial listing
        all_files = []

        while files:  # Iterate through the results of dbutils.fs.ls
            new_dirs = []
            for f in files:
                if f.path.endswith("/"):
                    new_dirs.append(f.path)  # Add directory to list for processing
                else:
                    all_files.append(f.path[trim_size:])  # Add file to results
            
            # Efficiently list subdirectories
            if new_dirs:
                files = []
                for dir in new_dirs:
                    files.extend(dbutils.fs.ls(dir)) # List all subdirectories in one go
            else:
                files = None # No more directories to process

        return all_files # Return all files at once as a list
    except Exception as e: # Catch any exceptions
        print(f"Error listing files in {path}: {e}")
        return [] # Return empty list in case of errors

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
    all_grant_files = [(file_path, my_config) for file_path in all_grant_files]

    # Add config to all_grant_files
    if already_processed is None:
        files_to_process = [f for f, _ in all_grant_files] # Just return all files if no metadata
    else:
        # Define the schema (this part is crucial and should be *inside* the function)
        schema = StructType([
            StructField("file_name", StringType(), True),
            StructField("config", StringType(), True)
        ])

        all_files_df = spark.createDataFrame(all_grant_files, schema=schema) # Use the schema here

        files_to_process_df = all_files_df.join(already_processed, ["file_name", "config"], "left_anti")
        files_to_process = [row.file_name for row in files_to_process_df.collect()]

    # Done
    return files_to_process


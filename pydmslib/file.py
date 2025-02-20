""" file.py contains methods that help with standardising reading/cleaning steps """
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
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

def crawl(path: str, trim_size=None):
    """
    Lists all files under a directory
    :param path: hdfs path
    :param trim_size: How many characters to trim returned paths by (defaults to path length)
    :return: yields strings
    """
    if path[-1] != "/":
        path = path + "/"
    if trim_size is None:
        if path[:6] == "dbfs:/":
            trim_size = len(path)
        elif path[0] == "/":
            trim_size = 5 + len(path)
        else:
            trim_size = 6 + len(path)
    for f in dbutils.fs.ls(path):
        if f.path[-1] != "/":
            yield f.path[trim_size:]
        else:
            for g in crawl(f.path, trim_size):
                yield g

def identify_to_process(metadata, process_dir, my_config=""):
    # Calculate files already processed
    if metadata is None:
        already_processed = set()
    else:
        if "config" not in metadata.columns:
            metadata = metadata.withColumn("config", f.lit(my_config))
        already_processed = set(
            [(x["file_name"], x["config"]) for x in metadata.select(f.col("file_name"), f.col("config")).collect()])

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
    all_grant_files = [(x, my_config) for x in all_grant_files]

    # Set difference to get all files to process
    files_to_process = list(set(all_grant_files) - already_processed)

    # Drop config names from files to process
    # Use a set as files will be processed more than once for different past configs
    files_to_process = list(set([x[0] for x in files_to_process]))

    # Done
    return files_to_process


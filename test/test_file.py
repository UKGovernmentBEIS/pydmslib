"""Tests file.py in pydmslib/file.py"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pydmslib.file import read_parquet_files_and_add_input_file


@pytest.fixture(scope="module", name="spark")
def init_spark():
    """Fetch local spark session for codespace testing"""
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


def test_read_parquet_files_and_add_input_file(spark, tmp_path):
    """Tests that read_parquet_files_and_add_input_file reads/adds filename column"""
    # Create a temporary directory
    temp_dir = tmp_path / "parquet_files"
    temp_dir.mkdir()

    # Create a sample DataFrame
    data = [("Alice", 1), ("Bob", 2)]
    columns = ["name", "id"]
    df = spark.createDataFrame(data, columns)

    # Write the DataFrame to Parquet
    df.write.mode("overwrite").parquet(str(temp_dir))

    # Read the Parquet files using the method
    result_df = read_parquet_files_and_add_input_file(str(temp_dir))

    # Check if the 'inputFile' column is added
    assert "inputFile" in result_df.columns

    # Check if the data is correct
    result_data = result_df.select("name", "id").collect()
    assert result_data == df.collect()

def test_read_parquet_files_with_pattern(spark, tmp_path):
    """Tests reads/adds filename column for valid parquets"""
    # Create a temporary directory
    temp_dir = tmp_path / "parquet_files"
    temp_dir.mkdir()

    # Create a sample DataFrame
    data = [("Alice", 1), ("Bob", 2)]
    columns = ["name", "id"]
    df = spark.createDataFrame(data, columns)

    # Write the DataFrame to Parquet with a specific pattern
    df.write.mode("overwrite").parquet(str(temp_dir / "data.parquet"))

    # Read the Parquet files using the method with a pattern
    result_df = read_parquet_files_and_add_input_file(str(temp_dir), pattern="/*.parquet")

    # Check if the 'inputFile' column is added
    assert "inputFile" in result_df.columns

    input_file_result = result_df.select("inputFile").collect()[0].inputFile

    assert input_file_result.endswith("parquet")

    # Check if the data is correct
    result_data = result_df.select("name", "id").collect()
    assert result_data == df.collect()

def test_read_parquet_files_invalid_directory():
    """Checks that AnalysisException is thrown if directory is incorect"""
    with pytest.raises(AnalysisException):
        read_parquet_files_and_add_input_file("invalid_directory")

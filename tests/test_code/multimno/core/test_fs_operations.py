import pytest
import tempfile
import os
from pathlib import Path

from multimno.core.spark_session import (
    check_if_data_path_exists,
    check_or_create_data_path,
    delete_file_or_folder,
    list_all_files_recursively,
    list_parquet_partition_col_values,
)

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def test_path_exists(spark):
    # Create a temporary directory to store test files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create temporary input and output files using tempfile
        with tempfile.NamedTemporaryFile(dir=temp_dir, mode="w", delete=False) as input_file:
            input_file.write("hello, world!")
            input_file_path = input_file.name
            assert check_if_data_path_exists(spark, input_file_path)
        assert check_if_data_path_exists(spark, temp_dir)

    assert not check_if_data_path_exists(spark, input_file_path)


def test_check_or_create_data_path(spark):
    # Create a temporary directory to store test files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = f"{temp_dir}/hello"
        # Create non existing dir and check
        check_or_create_data_path(spark, temp_path)
        assert check_if_data_path_exists(spark, temp_path)
        # Call again the function over existing dir
        check_or_create_data_path(spark, temp_path)
        assert check_if_data_path_exists(spark, temp_path)


def test_delete_file_or_folder(spark):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = f"{temp_dir}/hello"
        # Create non existing dir
        check_or_create_data_path(spark, temp_path)

        file_0_path = f"{temp_path}/test_0.txt"
        file_1_path = f"{temp_path}/test_1.txt"

        # Create two files
        for path in [file_0_path, file_1_path]:
            Path(path).touch()

        # Delete file
        delete_file_or_folder(spark, file_1_path)
        assert not check_if_data_path_exists(spark, file_1_path)

        # Delete dir
        delete_file_or_folder(spark, temp_path)

        assert not check_if_data_path_exists(spark, file_0_path)
        assert not check_if_data_path_exists(spark, temp_path)


def test_list_all_files_recursively(spark):

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = f"{temp_dir}/hello"
        # Create non existing dir
        check_or_create_data_path(spark, temp_path)

        file_0_path = f"{temp_path}/test_0.txt"
        file_1_path = f"{temp_path}/test_1.txt"

        # Create two files
        for path in [file_0_path, file_1_path]:
            Path(path).touch()

        expected_paths = {f"file:{x}" for x in [file_0_path, file_1_path]}
        paths = list_all_files_recursively(spark, temp_path)

        assert expected_paths == set(paths)


def test_list_parquet_partition_col_values(spark):
    # create a temporary directory to store parquet files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = f"{temp_dir}/hello"
        # Create non existing dir
        check_or_create_data_path(spark, temp_path)

        # Create a parquet file
        parquet_path = f"{temp_path}/test.parquet"
        df = spark.createDataFrame([(2, "a", "c"), (1, "b", "d")], ["col1", "col2", "col3"])
        df.write.partitionBy("col1").mode("overwrite").parquet(parquet_path)

        # Check the values of the partition column
        partition_col, partitions = list_parquet_partition_col_values(spark, parquet_path)
        assert partition_col == "col1"
        assert partitions == ["1", "2"]

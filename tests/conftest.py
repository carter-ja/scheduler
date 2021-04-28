"""Test configuration file for pytest
"""

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

@pytest.fixture(scope="session")
def spark_session():
    """Creates a reusable Spark Session

    Returns:
        SparkSession: Entrypoint to Spark application
    """
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark

@pytest.fixture
def student_schema():
    """Creates a reusable StructType schema

    Returns:
        StructType: Object that describes the csv header row values and types
    """
    return StructType([
    StructField("id", StringType()),
    StructField("fname", StringType()),
    StructField("lname", StringType()),
    StructField("email", StringType()),
    StructField("ssn", StringType()),
    StructField("address", StringType()),
    StructField("cid", StringType()),
])

@pytest.fixture
def teacher_schema():
    """Creates a reusable StructType Schema

    Returns:
        StructType: Object that describes the parquet header row values and types
    """
    return StructType([
    StructField("id", StringType()),
    StructField("fname", StringType()),
    StructField("lname", StringType()),
    StructField("email", StringType()),
    StructField("ssn", StringType()),
    StructField("address", StringType()),
    StructField("cid", StringType()),
])

@pytest.fixture
def student_schedule_schema():
    """Creates a reusable StructType Schema

    Returns:
        StructType: Object that describes the DataFrame header row values and types
    """
    return StructType([
    StructField("student_name", StringType(), False),
    StructField("teacher_name", StringType(), False),
    StructField("cid", StringType(), True),
])

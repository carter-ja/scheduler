"""Tests the scheduler.py package
"""

import pathlib
import pytest

from scheduler.scheduler import get_student_schema
from scheduler.scheduler import read_student_csv
from scheduler.scheduler import read_teacher_parquet
from scheduler.scheduler import join_student_teacher_dataframes
from scheduler.scheduler import write_df_to_json

@pytest.mark.usefixtures("student_schema")
def test_get_student_schema(student_schema):
    """Tests the get_student_schema function

    Args:
        student_schema (StructType): Object that describes the csv header
        row values and types
    """
    test_student_schema = get_student_schema()

    assert student_schema == test_student_schema

@pytest.mark.usefixtures("spark_session", "student_schema")
def test_read_student_csv_num_records(spark_session, student_schema):
    """Tests the read_student_csv function by comparing the number of
    records

    Args:
        spark_session (SparkSession): Entrypoint to Spark application
        student_schema (StructType): Object that describes the csv header
        row values and types
    """
    student_csv_df = read_student_csv(spark_session, student_schema)
    row_num = student_csv_df.count()

    assert row_num == 1000

@pytest.mark.usefixtures("spark_session", "student_schema")
def test_read_student_csv_header_values(spark_session, student_schema):
    """Tests the read_student_csv function by comparing header values

    Args:
        spark_session (SparkSession): Entrypoint to Spark application
        student_schema (StructType): Object that describes the csv header
        row values and types
    """
    student_csv_df = read_student_csv(spark_session, student_schema)

    assert student_csv_df.schema == student_schema

@pytest.mark.usefixtures("spark_session")
def test_read_teacher_parquet_num_records(spark_session):
    """Tests the read_teacher_parquet function by comparing the number of
    records

    Args:
        spark_session (SparkSession): Entrypoint to Spark application
    """
    teacher_parquet_df = read_teacher_parquet(spark_session)
    row_num = teacher_parquet_df.count()

    assert row_num == 10

@pytest.mark.usefixtures("spark_session", "teacher_schema")
def test_read_teacher_parquet_header_values(spark_session, teacher_schema):
    """Tests the read_teacher_parquet function by comparing header values

    Args:
        spark_session (SparkSession): Entrypoint to Spark application
        teacher_schema (StructType): Object that describes the parquet header
        row values and types
    """
    teacher_parquet_df = read_teacher_parquet(spark_session)

    assert teacher_parquet_df.schema == teacher_schema

@pytest.mark.usefixtures("spark_session", "student_schema")
def test_student_schedule_df_num_records(spark_session, student_schema):
    """Tests the join_student_teacher_dataframes function by comparing
    the number of records

    Args:
        spark_session (SparkSession): Entrypoint to Spark application
        student_schema (StructType): Object that describes the csv header
        row values and types
    """
    student_csv_df = read_student_csv(spark_session, student_schema)
    teacher_parquet_df = read_teacher_parquet(spark_session)
    student_schedule_df = join_student_teacher_dataframes(student_csv_df, teacher_parquet_df)
    row_num = student_schedule_df.count()

    assert row_num == 1000

@pytest.mark.usefixtures("spark_session", "student_schema", "student_schedule_schema")
def test_student_schedule_df_header_values(spark_session, student_schema, student_schedule_schema):
    """Tests the read_student_csv function by comparing header values

    Args:
        spark_session (SparkSession): Entrypoint to Spark application
        student_schema (StructType): Object that describes the csv header
        row values and types
        student_schedule_schema (StructType): Object that describes the
        DataFrame header row values and types
    """
    student_csv_df = read_student_csv(spark_session, student_schema)
    teacher_parquet_df = read_teacher_parquet(spark_session)
    student_schedule_df = join_student_teacher_dataframes(student_csv_df, teacher_parquet_df)

    assert student_schedule_df.schema == student_schedule_schema

@pytest.mark.usefixtures("spark_session", "student_schema", "student_schedule_schema")
def test_integration(spark_session, student_schema):
    """Tests all functionality of the application

    Args:
        spark_session (SparkSession): Entrypoint to Spark application
        student_schema (StructType): Object that describes that csv header
        row values and types
    """
    student_csv_df = read_student_csv(spark_session, student_schema)
    teacher_parquet_df = read_teacher_parquet(spark_session)
    student_schedule_df = join_student_teacher_dataframes(student_csv_df, teacher_parquet_df)

    write_df_to_json(student_schedule_df)

    file_path = pathlib.Path("data_output/")
    file_count = 0
    file_name = None
    for child in file_path.iterdir():
        if child.suffix == ".json":
            file_count += 1
            file_name = child

    assert file_count == 1
    assert file_name.is_file()

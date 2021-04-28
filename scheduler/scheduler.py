"""scheduler takes in two files, a csv with student data, and a parquet
with teacher data, and combines the two files into one Dataframe.
It handles ambiguous column names, and then converts the DataFrame
into a json file in the data_output directory
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType

__all__ = [
    'get_student_schema', 'get_spark_session', 'read_student_csv',
    'read_teacher_parquet', 'join_student_teacher_dataframes', 'write_df_to_json'
    ]

def get_student_schema():
    """Uses StructType and StructField objects to create a schema that describes
    the student csv

    Returns:
        StructType: Object that describes the csv header row values and types
    """
    student_schema = StructType([
        StructField("id", StringType()),
        StructField("fname", StringType()),
        StructField("lname", StringType()),
        StructField("email", StringType()),
        StructField("ssn", StringType()),
        StructField("address", StringType()),
        StructField("cid", StringType()),
    ])

    return student_schema

def get_spark_session():
    """Creates a SparkSession if one does not already exist, otherwise grabs the
    existing SparkSession.

    Returns:
        SparkSession: Entrypoint to Spark application
    """
    spark = SparkSession \
        .builder \
        .appName("scheduler") \
        .master("local[3]") \
        .getOrCreate()

    return spark

def read_student_csv(spark, schema):
    """Reads in a csv with student data, splits by underscore delimiter,
    assumes header is present.

    Args:
        spark (SparkSession): Entrypoint to Spark application
        schema (StructType): Object that describes the csv header row values and types

    Returns:
        pyspark.sql.DataFrame: DataFrame object returned from pyspark.sql API
    """
    student_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", "_") \
        .schema(schema) \
        .load("data/students*.csv")

    return student_df

def read_teacher_parquet(spark):
    """Reads in a parquet file with teacher data

    Args:
        spark (SparkSession): Entrypoint to Spark application

    Returns:
        pyspark.sql.DataFrame: DataFrame object returned from pyspark.sql API
    """
    teacher_df = spark.read \
        .format("parquet") \
        .load("data/teachers*.parquet")

    return teacher_df

def join_student_teacher_dataframes(student_df, teacher_df):
    """Takes in a student csv file and a teacher parquet file and returns an inner joined
    DataFrame that handles relevant ambiguous names

    Args:
        student_df (pyspark.sql.DataFrame): A DataFrame of student data
        teacher_df (pyspark.sql.DataFrame): A DataFrame of teacher data

    Returns:
        pyspark.sql.DataFrame: A DataFrame composed of an inner join between student_df
        and teacher_df
    """
    join_expression = student_df.cid == teacher_df.cid

    teacher_renamed_df = teacher_df.withColumnRenamed("fname", "teacher_fname") \
        .withColumnRenamed("lname", "teacher_lname")

    student_schedule_df = student_df.join(teacher_renamed_df, join_expression, "inner") \
        .drop(teacher_df.cid) \
        .select(
            func.concat_ws(" ", student_df.fname, student_df.lname).alias("student_name"),
            func.concat_ws(" ", teacher_renamed_df.teacher_fname,
                teacher_renamed_df.teacher_lname).alias("teacher_name"),
            "cid"
        )

    return student_schedule_df

def write_df_to_json(student_schedule_df):
    """Uses pyspark.sql.DataFrame.write API to write the input DataFrame to json

    Args:
        df (pyspark.sql.DataFrame): A DataFrame that is to be written to
        data_output directory
    """
    student_schedule_df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "data_output/") \
        .save()

def main():
    """Main method of this __name__ file
    """
    spark = get_spark_session()
    student_schema = get_student_schema()
    student_df = read_student_csv(spark, schema=student_schema)
    teacher_df = read_teacher_parquet(spark)
    student_schedule_df = join_student_teacher_dataframes(student_df, teacher_df)

    write_df_to_json(student_schedule_df)


if __name__ == "__main__":
    main()

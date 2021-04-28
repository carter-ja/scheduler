from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType

def get_student_schema():
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
    spark = SparkSession \
        .builder \
        .appName("scheduler") \
        .master("local[3]") \
        .getOrCreate()

    return spark

def read_student_csv(spark, schema):
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("delimiter", "_") \
            .schema(schema) \
            .load("data/students*.csv")

        return df

def read_teacher_parquet(spark):
    teacher_df = spark.read \
        .format("parquet") \
        .load("data/teachers*.parquet")

    return teacher_df

def join_student_teacher_dataframes(student_df, teacher_df):
    join_expression = student_df.cid == teacher_df.cid

    teacher_renamed_df = teacher_df.withColumnRenamed("fname", "teacher_fname") \
        .withColumnRenamed("lname", "teacher_lname")

    student_schedule_df = student_df.join(teacher_renamed_df, join_expression, "inner") \
        .drop(teacher_df.cid) \
        .select(
            func.concat_ws(" ", student_df.fname, student_df.lname).alias("student_name"),
            func.concat_ws(" ", teacher_renamed_df.teacher_fname, teacher_renamed_df.teacher_lname).alias("teacher_name"),
            "cid"
        )

    return student_schedule_df

def write_df_to_json(df):
    df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "data_output/") \
        .save()

def main():
    spark = get_spark_session()
    student_schema = get_student_schema()
    student_df = read_student_csv(spark, schema=student_schema)
    teacher_df = read_teacher_parquet(spark)
    student_schedule_df = join_student_teacher_dataframes(student_df, teacher_df)

    write_df_to_json(student_schedule_df)


if __name__ == "__main__":
    main()


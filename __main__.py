"""Main entrypoint into Spark application
"""
from scheduler import get_spark_session
from scheduler import get_student_schema
from scheduler import read_student_csv
from scheduler import read_teacher_parquet
from scheduler import join_student_teacher_dataframes
from scheduler import write_df_to_json

def main():
    """Main method
    """
    spark = get_spark_session()
    student_schema = get_student_schema()
    student_df = read_student_csv(spark, schema=student_schema)
    teacher_df = read_teacher_parquet(spark)
    student_schedule_df = join_student_teacher_dataframes(student_df, teacher_df)

    write_df_to_json(student_schedule_df)


if __name__ == "__main__":
    main()

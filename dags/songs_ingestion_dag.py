from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col, when, count, isnan, year, month, regexp_replace, upper,length, split, lower,initcap,trim
from pyspark.sql.functions import broadcast

#Step:1 - Initialize Spark Session
def get_spark_session():
    return SparkSession.builder.appName("StreamPulse_Analytics_ETL").config("spark.jars.packages", "org.postgresql:postgresql:42.3.1") \
        .getOrCreate()

# step:2 - Extract CSV's

def extract_csv():
    spark = get_spark_session()
    df = spark.read.csv("/opt/airflow/data/songs.csv", header=True, inferSchema=True)
    df.write.mode("overwrite").parquet("/opt/airflow/tmp/songs")
    spark.stop()

#step:3 - Transform with Pyspark

def transform_with_pyspark():    
    spark = get_spark_session()
    df = spark.read.parquet("/opt/airflow/tmp/songs")
    # Trim whitespace
    df = df.withColumn("song_title", trim(col("song_title")))
    df = df.withColumn("genre", trim(col("genre")))

    # Clean and format text
    df = df.withColumn("song_title", initcap(col("song_title")))
    df = df.withColumn("genre", initcap(col("genre")))
    df = df.withColumn("song_title", regexp_replace(col("song_title"), r"[^a-zA-Z0-9 ]", ""))

    # Cast data types
    df = df.withColumn("song_id", col("song_id").cast("int"))
    df = df.withColumn("artist_id", col("artist_id").cast("int"))
    df = df.withColumn("duration_sec", col("duration_sec").cast("int"))

    # Filter invalid durations
    df = df.filter(col("duration_sec").isNotNull() & (col("duration_sec") > 60))

    # Validate song title & artist_id
    df = df.filter(col("song_title").isNotNull() & (length(col("song_title")) > 1))
    df = df.filter(col("artist_id").isNotNull() & (col("artist_id") > 0))

    # Remove duplicates
    df = df.dropDuplicates(["song_id"])

    # Add a length category
    df = df.withColumn(
        "length_category",
        when(col("duration_sec") < 150, "Short")
        .when((col("duration_sec") >= 150) & (col("duration_sec") <= 240), "Standard")
        .otherwise("Long")
    )
    # Write cleaned data
    df.write.mode("overwrite").parquet("/opt/airflow/tmp/final_songs_data")
    spark.stop()

def quality_checks():
    spark = get_spark_session()
    df = spark.read.parquet("/opt/airflow/tmp/final_songs_data")

    if df.count() == 0:
        print("No records after transformation. Quality check failed!")

    print("Nulls in songs:")
    df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns]).show()

    print(f"Duplicate rows: {df.count() - df.dropDuplicates().count()}")
    spark.stop()


def load_to_postgresSQL():
    spark = get_spark_session()

    final_df = spark.read.parquet("/opt/airflow/tmp/final_songs_data")

    final_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/StreamPulse_Analytics") \
        .option("dbtable", "songs_data") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    spark.stop()

with DAG(
    dag_id="import_songs_data_to_DB",
    start_date=datetime(2025, 6, 10),
    schedule=None,
    catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv
        )
    transform_task = PythonOperator(
        task_id="transform_with_pyspark",
        python_callable=transform_with_pyspark
    )
    load_task = PythonOperator(
        task_id="load_to_postgresSQL",
        python_callable=load_to_postgresSQL
    )
    quality_task = PythonOperator(
        task_id="quality_checks",
        python_callable=quality_checks
    )


extract_task >> transform_task >> quality_task >> load_task




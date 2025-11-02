from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col, when, count, isnan, year, month, regexp_replace, upper , split, lower,initcap,trim
from pyspark.sql.functions import broadcast

#Step:1 - Initialize Spark Session
def get_spark_session():
    return SparkSession.builder.appName("StreamPulse_Analytics_ETL").config("spark.jars.packages", "org.postgresql:postgresql:42.3.1") \
        .getOrCreate()

# step:2 - Extract CSV's

def extract_csv():
    spark = get_spark_session()
    df = spark.read.csv("/opt/airflow/data/artists.csv", header=True, inferSchema=True)
    df.write.mode("overwrite").parquet("/opt/airflow/tmp/artists")
    spark.stop()

#step:3 - Transform with Pyspark

def transform_with_pyspark():    
    spark = get_spark_session()
    df = spark.read.parquet("/opt/airflow/tmp/artists")

    df = df.withColumn("artist_name", initcap(trim(col("artist_name"))))
    df = df.withColumn("artist_name", regexp_replace(col("artist_name"), r"[^a-zA-Z\s.]", ""))

    df = df.withColumn("country", initcap(trim(col("country"))))
    df = df.withColumn(
    "country",
    when(lower(col("country")).isin("india"), "India")
    .when(lower(col("country")).isin("united states", "usa", "us"), "United States")
    .when(lower(col("country")).isin("united kingdom", "uk"), "United Kingdom")
    .otherwise("Other")
    )

    df = df.filter(col("artist_id").cast("int").isNotNull() & (col("artist_id") > 0))
    df = df.dropDuplicates(["artist_id"])
    
    # Write cleaned data
    df.write.mode("overwrite").parquet("/opt/airflow/tmp/final_artists_data")
    spark.stop()

def quality_checks():
    spark = get_spark_session()
    df = spark.read.parquet("/opt/airflow/tmp/final_artists_data") 
    print("Nulls in artists:")
    df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns]).show()

    print(f"Duplicate rows: {df.count() - df.dropDuplicates().count()}")
    spark.stop()


def load_to_postgresSQL():
    spark = get_spark_session()

    final_df = spark.read.parquet("/opt/airflow/tmp/final_artists_data")

    final_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/StreamPulse_Analytics") \
        .option("dbtable", "artists_data") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    spark.stop()

with DAG(
    dag_id="import_artists_data_to_DB",
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


extract_task >> transform_task >> quality_task  >> load_task



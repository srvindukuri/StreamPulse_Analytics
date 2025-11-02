from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import os
from pyspark.sql.types import FloatType, DoubleType
from pyspark.sql.functions import col, when, count, isnan, year, month, regexp_replace, upper, split, lit,initcap,trim

from pyspark.sql.functions import broadcast

#Step:1 - Initialize Spark Session
def get_spark_session():
    return SparkSession.builder.appName("StreamPulse_Analytics_ETL").config("spark.jars.packages", "org.postgresql:postgresql:42.3.1") \
        .getOrCreate()

# step:2 - Extract CSV's

def extract_csv():
    spark = get_spark_session()
    df = spark.read.csv("/opt/airflow/data/users.csv", header=True, inferSchema=True)
    df.write.mode("overwrite").parquet("/opt/airflow/tmp/users")
    spark.stop()

#step:3 - Transform with Pyspark

def transform_with_pyspark():    
    spark = get_spark_session()
    df = spark.read.parquet("/opt/airflow/tmp/users")

     # Split full name
    df = df.withColumn("first_name", split(col("name"), " ").getItem(0)) \
           .withColumn("last_name", split(col("name"), " ").getItem(1))
    
    df = df.withColumn("last_name",when(col("last_name").isNull(), "").otherwise(col("last_name"))
    )

    # Title case names
    df = df.withColumn("first_name", initcap(col("first_name"))) \
           .withColumn("last_name", initcap(col("last_name")))

    # Clean and normalize country text
    df = df.withColumn("country", upper(trim(col("country"))))
    # Validate age range
    df = df.filter((col("age") >= 13) & (col("age") <= 85))

    # User segmentation by age group
    df = df.withColumn(
        "user_segment",
        when(col("age") < 18, "Teen")
        .when(col("age").between(18, 40), "Adult")
        .when(col("age").between(40, 60), "Middle adulthood")
        .otherwise("Senior")
    )
    df = df.fillna({"country": "UNKNOWN"})
    df.write.mode("overwrite").parquet("/opt/airflow/tmp/final_users_data")
    spark.stop()

def quality_checks():
    from pyspark.sql.types import FloatType, DoubleType

    spark = get_spark_session()
    df = spark.read.parquet("/opt/airflow/tmp/final_users_data")

    print("Nulls in users:")

    # Identify numeric columns
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (FloatType, DoubleType))]

    # Safe null + NaN count
    null_nan_counts = df.select([
        count(
            when(
                col(c).isNull() | (isnan(col(c)) if c in numeric_cols else lit(False)),
                c
            )
        ).alias(c)
        for c in df.columns
    ])

    null_nan_counts.show()

    print(f"Duplicate rows: {df.count() - df.dropDuplicates().count()}")
    spark.stop()

def load_to_postgresSQL():
    spark = get_spark_session()

    final_df = spark.read.parquet("/opt/airflow/tmp/final_users_data")

    final_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/StreamPulse_Analytics") \
        .option("dbtable", "users_data") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    spark.stop()

with DAG(
    dag_id="import_users_data_to_PGDB",
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


extract_task >> transform_task  >> quality_task >> load_task



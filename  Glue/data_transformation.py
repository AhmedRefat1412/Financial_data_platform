import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

# تهيئة الـ Glue Job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# الـ Buckets
BRONZE_BUCKET = "s3://bronze-financial-data-ahmed/raw/"
SILVER_BUCKET = "s3://silver-financial-data-ahmed/processed/"

print(" بقرأ الداتا من Bronze...")
df_train = spark.read.csv(BRONZE_BUCKET + "fraudTrain.csv", header=True, inferSchema=True)
df_test = spark.read.csv(BRONZE_BUCKET + "fraudTest.csv", header=True, inferSchema=True)

# دمج الـ Train والـ Test
df = df_train.union(df_test)
print(f"عدد الصفوف الكلي: {df.count()}")

# Data Quality Checks
null_count = df.filter(F.col("amt").isNull()).count()
negative_count = df.filter(F.col("amt") <= 0).count()

if null_count > 0 or negative_count > 0:
    raise Exception(f" Data Quality Failed! Nulls: {null_count}, Negatives: {negative_count}")
print(" Quality Checks عدت صح!")

# تنظيف وتحويل الداتا
df_clean = df \
    .withColumn("trans_date", F.to_date(F.col("trans_date_trans_time"))) \
    .withColumn("trans_hour", F.hour(F.col("trans_date_trans_time"))) \
    .withColumn("trans_month", F.month(F.col("trans_date_trans_time"))) \
    .withColumn("trans_year", F.year(F.col("trans_date_trans_time"))) \
    .withColumn("trans_dayofweek", F.dayofweek(F.col("trans_date_trans_time"))) \
    .withColumn("age", F.year(F.current_date()) - F.year(F.to_date(F.col("dob"), "yyyy-MM-dd"))) \
    .withColumn("is_fraud", F.col("is_fraud").cast(IntegerType())) \
    .withColumn("amt", F.col("amt").cast(DoubleType()))

# شيل الـ Columns اللي مش محتاجينها
cols_to_drop = ["_c0", "unix_time", "trans_num", "dob", "trans_date_trans_time"]
df_clean = df_clean.drop(*cols_to_drop)

# print(f"الداتا النظيفة فيها {df_clean.count()} صف")

# اكتب على Silver
df_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_BUCKET)

job.commit()

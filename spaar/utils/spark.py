from pyspark.sql import SparkSession

def local_spark():
    from boto3 import Session
    # attempting to get AWS creds via Boto3
    session = Session()
    credentials = session.get_credentials().get_frozen_credentials()

    return SparkSession \
    .builder \
    .appName("local") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.13:3.2.0") \
    .config("spark.hadoop.fs.s3a.access.key", credentials.access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", credentials.secret_key) \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()
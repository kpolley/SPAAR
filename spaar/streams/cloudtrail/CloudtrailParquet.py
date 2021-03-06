from datetime import datetime, timezone
from spaar.schemas.cloudtrail import raw 
from spaar.streams.base import Stream
from spaar.config import Config
import pyspark.sql.functions as F
from pyspark.sql.types import *
import time

CLOUDTRAIL_RAW_BUCKET = "s3a://aws-cloudtrail-logs-526392422370-def493f2"
CLOUDTRAIL_DATALAKE_DIR = Config.get('s3_bucket') + '/cloudtrail'
CLOUDTRAIL_CHECKPOINT_DIR = Config.get('s3_bucket') + '/cloudtrail_checkpoint'

def get_cloudtrail_path(date=datetime.now(timezone.utc)):
    """
    Returns the AWS object path regex for a given day
    """
    year = date.year
    month = f"{date.month:02}"
    day = f"{date.day:02}"

    return f"{CLOUDTRAIL_RAW_BUCKET}/AWSLogs/*/CloudTrail/*/{year}/{month}/{day}/*.json.gz"

class CloudtrailParquet(Stream):
    path = get_cloudtrail_path()
    schema = raw.schema

    def __init__(self, spark):
        Stream.__init__(self, spark)
        
    def read(self):
        self._df = self._spark.readStream \
            .option("maxFilesPerTrigger", 100) \
            .schema(raw.schema) \
            .json(self.path)

    def transform(self):
        # explode the Records array
        self._df = self._df \
            .select(F.explode("Records").alias("record")) \
            .select("record.*")
        
        # convert `eventTime` to timestamp datatype, then add columns `ts`, `dt`, and `hr`
        self._df = self._df \
            .withColumn('ts', F.to_timestamp("eventTime", "yyyy-MM-dd'T'HH:mm:ss'Z'").cast("timestamp")) \
            .withColumn("dt", F.col('ts').cast('date')) \
            .withColumn("hr", F.hour('ts'))

    def load(self):
        self._df = self._df.writeStream \
            .format("parquet") \
            .option("path", CLOUDTRAIL_DATALAKE_DIR) \
            .partitionBy("dt", "hr") \
            .trigger(processingTime="300 seconds") \
            .option("checkpointLocation", CLOUDTRAIL_CHECKPOINT_DIR)

    def run(self):
        """
        Spark Streaming wrapper logic to start/stop stream for new date
        """
        self.read()
        self.transform()
        self.load()
        self._df = self._df.start()

        while True:
            if get_cloudtrail_path() != self.path:
                self._df = self._df.stop()
                self.path = get_cloudtrail_path()
            
                self.read()
                self.transform()
                self.load()
                self._df = self._df.start()

            print(f"Streaming Status: {self._df.status}")
            time.sleep(300)

job = CloudtrailParquet
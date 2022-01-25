from datetime import datetime, timezone
from schemas import cloudtrail 
from streams.base import Stream
import pyspark.sql.functions as F
from pyspark.sql.types import *
import time

RAW_BUCKET = "s3a://aws-cloudtrail-logs-526392422370-def493f2"
DATALAKE_DIR = "s3a://kpolley-datalake"

def get_cloudtrail_path(date=datetime.now(timezone.utc)):
    """
    Returns the AWS object path regex for a given day
    """
    year = date.year
    month = f"{date.month:02}"
    day = f"{date.day:02}"

    return f"{RAW_BUCKET}/AWSLogs/*/CloudTrail/*/{year}/{month}/{day}/*.json.gz"

class CloudtrailParquet(Stream):
    def __init__(self, spark):
        self._path = get_cloudtrail_path()
        Stream.__init__(self, spark)
        
    def read(self):
        self._df = self._spark.readStream \
            .option("maxFilesPerTrigger", 100) \
            .schema(cloudtrail.schema) \
            .json(self._path)

    def transform(self):
        # explode the Records array
        self._df = self._df \
            .select(F.explode("Records").alias("record")) \
            .select("record.*")
        
        # convert `eventTime` to timestamp datatype, then add columns `ts`, `dt`, and `hr`
        self._df = self._df \
            .withColumn('ts', F.unix_timestamp("eventTime", "yyyy-MM-dd'T'HH:mm:ss'Z'").cast("timestamp")) \
            .withColumn("dt", F.col('ts').cast('date')) \
            .withColumn("hr", F.hour('ts'))

    def load(self):
        self._df.writeStream \
            .format("parquet") \
            .option("path", DATALAKE_DIR + "/cloudtrail/") \
            .partitionBy("dt", "hr") \
            .trigger(processingTime="60 seconds") \
            .option("checkpointLocation", DATALAKE_DIR + "/cloudtrail_checkpoint/")

    def run(self):
        """
        Spark Streaming wrapper logic to start/stop stream for new date
        """
        self.read()
        self.transform()
        self.load()
        self._df.start()

        while True:
            if get_cloudtrail_path() != self._path:
                self._df.stop()
                self._path = get_cloudtrail_path()
            
                self.read()
                self.transform()
                self.load()
                self._df.start()

            time.sleep(300)

stream = CloudtrailParquet
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

DATALAKE_DIR = "s3a://kpolley-datalake"

class Detection:
    def __init__(self, spark, path, schema):
        self._spark = spark
        self._df = None

        self._path = path
        self._schema = schema

    def read(self):
        self._df = self._spark.readStream \
            .option("maxFilesPerTrigger", 10) \
            .schema(self._schema) \
            .parquet(self._path)

    def run_trigger(self):
        return True

    def generate_alert(self):
        #TODO: logic to send alert some place
        self._df = self._df.writeStream \
            .option("checkpointLocation", f"{DATALAKE_DIR}/aws_unused_region_checkpoint") \
            .trigger(processingTime="60 seconds") \
            .foreach(lambda message: print(message))

    def run(self):
        self.read()
        self.run_trigger()
        self.generate_alert()

        self._df.start().awaitTermination()

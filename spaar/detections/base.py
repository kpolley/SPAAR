from spaar.utils.alert import send_sns, row_to_string
from spaar.config import Config

DATALAKE_DIR = Config.get('s3_bucket')

class Detection:
    def __init__(self, spark):
        self._spark = spark
        self._df = None

    def read(self):
        self._df = self._spark.readStream \
            .option("maxFilesPerTrigger", 100) \
            .schema(self.schema) \
            .parquet(self.s3_bucket)

    def run_trigger(self):
        return True

    def generate_alert(self):
        #TODO: logic to send alert some place
        self._df = self._df.writeStream \
            .option("checkpointLocation", f"{DATALAKE_DIR}/{self.__class__.__name__}_checkpoint") \
            .trigger(processingTime="60 seconds") \
            .foreach(lambda row: send_sns(self.alert_title, row_to_string(row)))

    def run(self):
        self.read()
        self.run_trigger()
        self.generate_alert()

        self._df.start().awaitTermination()

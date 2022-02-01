from spaar.detections.base import Detection
from spaar.config import Config
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DateType, TimestampType, BooleanType


@F.udf(BooleanType())
def should_trigger(region):
    """
    Logic used to define what causes an alert
    """
    allowed_regions = [
        'us-east-1'
    ]

    if region not in allowed_regions:
        return True
    return False
    
class UnusedRegion(Detection):
    # Title of the alert
    alert_title = "AWS Activity in Unused Region"

    # fields used for detection logic/alert output
    schema = StructType() \
        .add('eventName', StringType()) \
        .add('eventSource', StringType()) \
        .add('awsRegion', StringType()) \
        .add('errorMessage', StringType()) \
        .add('sourceIPAddress', StringType()) \
        .add('eventID', StringType()) \
        .add('userIdentity.arn', StringType()) \
        .add('userAgent', StringType()) \
        .add('dt', DateType()) \
        .add('ts', TimestampType())
    
    # input directory
    input_dir = Config.get('s3_bucket') + '/cloudtrail'

    def __init__(self, spark):
        Detection.__init__(self, spark)

    def run_trigger(self):
        self._df = self._df.filter(should_trigger(F.col('awsRegion')))

detection = UnusedRegion
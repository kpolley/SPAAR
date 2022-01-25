from detections.base import Detection
from pyspark.sql.types import StructType, StringType, DateType, TimestampType

INPUT_PATH = "s3a://kpolley-datalake/cloudtrail"
SCHEMA = StructType() \
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

# Allowlisted regions
USED_REGIONS = [
    'us-east-1'
]

class CloudTrailRootLogin(Detection):
    def __init__(self, spark):
        Detection.__init__(self, spark, INPUT_PATH, SCHEMA)

    def should_trigger(region):
        if region not in USED_REGIONS:
            return True
        return False

detection = CloudTrailRootLogin
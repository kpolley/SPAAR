from spaar.detections.base import Detection
from spaar.utils.spark import schema_subset
from spaar.schemas.cloudtrail import bronze
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

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
    cloudtrail_fields = [
        'eventName',
        'eventSource',
        'awsRegion',
        'errorMessage',
        'sourceIPAddress',
        'eventID',
        'userIdentity.arn',
        'userAgent',
        'dt',
        'ts'
    ]
    schema = schema_subset(bronze.schema, cloudtrail_fields)
    
    # s3 bucket to read from
    s3_bucket = bronze.s3_bucket

    def __init__(self, spark):
        Detection.__init__(self, spark)

    def run_trigger(self):
        self._df = self._df.filter(should_trigger(F.col('awsRegion')))

job = UnusedRegion
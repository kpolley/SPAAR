from spaar.detections.base import Detection
from spaar.utils.spark import schema_subset
from spaar.schemas.cloudtrail import bronze
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

@F.udf(BooleanType())
def is_failed_login(event_name, error):
    if event_name == 'ConsoleLogin' and error != None:
        return True
    return False

@F.udf(BooleanType())
def should_trigger(count):
    if count >= 3:
        return True
    return False
    
class HighFailLogins(Detection):
    # Title of the alert
    alert_title = "High Number of Failed Logins"

    # fields used for detection logic/alert output
    cloudtrail_fields = [
        'eventName',
        'errorMessage',
        'sourceIPAddress',
        'userIdentity.userName',
        'ts'
    ]
    schema = schema_subset(bronze.schema, cloudtrail_fields)
    
    # s3 bucket to read from
    s3_bucket = bronze.s3_bucket

    def __init__(self, spark):
        Detection.__init__(self, spark)

    def run_trigger(self):
        # filters for failed login attempts
        # counts number of failed login by username over a 15 minute window
        self._df = self._df \
            .filter(is_failed_login(F.col('eventName'), F.col('errorMessage'))) \
            .groupBy(F.window(F.col('ts'), "15 minutes"), F.col('userIdentity.userName')) \
            .count()

        # filters out rows which do not meet our trigger threshold
        self._df = self._df.filter(should_trigger(F.col('count')))

job = HighFailLogins
from spaar.detections.base import Detection
from spaar.utils.spark import schema_subset
from spaar.schemas.cloudtrail import bronze
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType
import geoip2.database

@F.udf(StringType())
def get_country_code(ip_address):
    reader = geoip2.database.Reader('assets/GeoLite2-City.mmdb')
    try:
        print(reader.city(ip_address).country.iso_code)
        return reader.city(ip_address).country.iso_code
    except:
        pass
    return None

@F.udf(BooleanType())
def should_trigger(country_code):
    """
    Logic used to define what causes an alert
    """
    if country_code and country_code != 'US':
        return True
    return False
    
class ForeignIP(Detection):
    # Title of the alert
    alert_title = "AWS Activity from Foreign IP Address"

    # fields used for detection logic/alert output
    cloudtrail_fields = [
        'eventName',
        'eventSource',
        'awsRegion',
        'errorMessage',
        'sourceIPAddress',
        'userIdentity.arn',
        'userAgent',
        'ts'
    ]
    schema = schema_subset(bronze.schema, cloudtrail_fields)
    
    # s3 bucket to read from
    s3_bucket = bronze.s3_bucket

    def __init__(self, spark):
        Detection.__init__(self, spark)

    def run_trigger(self):
        # populate df with country code and filter
        self._df = self._df \
            .withColumn('country_code', get_country_code(F.col('sourceIPAddress'))) \
            .filter(should_trigger(F.col('country_code')))

job = ForeignIP
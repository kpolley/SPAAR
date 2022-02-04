from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

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

def schema_subset(schema, requested_fields):
    """
    Input: A defined StructType schema and a list of fields 
    Output: A StructType schema object with just those fields
    """
   
    def schema_subset_helper(schema, nested_field):
        """
        helper function which handles nested fields ex. userIdentity.arn
        """
        schema = schema['fields']
        this_field = nested_field.pop(0)
        for key in schema:
            if key['name'] == this_field:
                if len(nested_field) == 0:
                    return key
                return schema_subset_helper(key['type'], nested_field)

    ret = {'fields': []}
    schema = schema.jsonValue()    
    for requested_field in requested_fields:
        requested_field = requested_field.split('.')
        field = schema_subset_helper(schema, requested_field)
        ret['fields'].append(field)

    return StructType.fromJson(ret)
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
   
    def schema_subset_helper(schema, nested_fields):
        """
        helper function which handles nested fields ex. userIdentity.arn
        """
        this_field = nested_fields.pop(0)
        for key in schema['fields']:
            if key['name'] == this_field:
                if len(nested_fields) == 0:
                    return key
                key['type']['fields'] = [schema_subset_helper(key['type'], nested_fields)]
                return key

    ret = {'fields': []}
    schema = schema.jsonValue()    
    for requested_field in requested_fields:
        field = schema_subset_helper(schema, requested_field.split('.'))
        ret['fields'].append(field)

    return StructType.fromJson(ret)
from pyspark.sql.types import *

schema = StructType() \
            .add("Records", ArrayType(StructType() \
                .add("additionalEventData", StringType()) \
                .add("apiVersion", StringType()) \
                .add("awsRegion", StringType()) \
                .add("errorCode", StringType()) \
                .add("errorMessage", StringType()) \
                .add("eventID", StringType()) \
                .add("eventName", StringType()) \
                .add("eventSource", StringType()) \
                .add("eventTime", StringType()) \
                .add("eventType", StringType()) \
                .add("eventVersion", StringType()) \
                .add("readOnly", BooleanType()) \
                .add("recipientAccountId", StringType()) \
                .add("requestID", StringType()) \
                .add("requestParameters", MapType(StringType(), StringType())) \
                .add("resources", ArrayType(StructType() \
                .add("ARN", StringType()) \
                .add("accountId", StringType()) \
                .add("type", StringType()) \
                )) \
                .add("responseElements", MapType(StringType(), StringType())) \
                .add("sharedEventID", StringType()) \
                .add("sourceIPAddress", StringType()) \
                .add("serviceEventDetails", MapType(StringType(), StringType())) \
                .add("userAgent", StringType()) \
                .add("userIdentity", StructType() \
                .add("accessKeyId", StringType()) \
                .add("accountId", StringType()) \
                .add("arn", StringType()) \
                .add("invokedBy", StringType()) \
                .add("principalId", StringType()) \
                .add("sessionContext", StructType() \
                    .add("attributes", StructType() \
                    .add("creationDate", StringType()) \
                    .add("mfaAuthenticated", StringType()) \
                    ) \
                    .add("sessionIssuer", StructType() \
                    .add("accountId", StringType()) \
                    .add("arn", StringType()) \
                    .add("principalId", StringType()) \
                    .add("type", StringType()) \
                    .add("userName", StringType()) \
                    )
                ) \
                .add("type", StringType()) \
                .add("userName", StringType()) \
                .add("webIdFederationData", StructType() \
                    .add("federatedProvider", StringType()) \
                    .add("attributes", MapType(StringType(), StringType())) \
                )
                ) \
                .add("vpcEndpointId", StringType())))
import boto3
import json

def send_sns(subject, message):
    client = boto3.client('sns', region_name='us-east-1')
    
    client.publish(
        TopicArn='arn:aws:sns:us-east-1:526392422370:SPAAR-Alerts',
        Subject=subject,
        Message=message 
    )

def row_to_string(row):
    return json.dumps(row.asDict(), indent=4, default=str)
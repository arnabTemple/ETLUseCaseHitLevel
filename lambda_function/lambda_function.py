import csv
import boto3
import io

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print("Bucket = " + bucket)
    print("Key = " + key)

    # Get the s3 object for processing
    response = s3_client.get_object(Bucket=bucket, Key=key)

    data = response['Body'].read().decode('utf-8')
    reader = csv.reader(io.StringIO(data), delimiter='\t')

    next(reader)
    for row in reader:
        print(row)

import csv
import boto3
import io

s3_client = boto3.client('s3')
spark_client = boto3.client('emr-serverless')


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print("Bucket = " + bucket)
    print("Key = " + key)
    print("File Path = s3://" + bucket + "/" + key)

    # Get the s3 object for processing and sample print from file
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')
    reader = csv.reader(io.StringIO(data), delimiter='\t')
    next(reader)
    for row in reader:
        print(row)

    response = spark_client.run_job_flow(
        name='etl_use_case_hit_level',
        applicationId='00f8i91mv7194809',
        executionRoleArn='arn:aws:iam::437288517367:role/lambda_s3_cloudwatch_full',
        jobDriver={
            'sparkSubmit': {
                'entryPoint': 's3://hit-level-etl-use-case/codebase/ProcessHitLevelSpark.py',
                'entryPointArguments': [
                    "s3://" + bucket + "/" + key, "s3://hit-level-etl-use-case/output"
                ],
                'sparkSubmitParameters': ''
            }
        }
    )

    print("Response = " + str(response))



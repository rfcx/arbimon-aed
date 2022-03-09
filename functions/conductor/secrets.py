import boto3
import base64
import json
from botocore.exceptions import ClientError


def aws_secrets(secret_name):

    session = boto3.session.Session()
    region_name = "us-east-1"
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    response = {}
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    response = json.loads(secret)

    return response
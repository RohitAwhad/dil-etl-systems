import boto3
import json
import sys


def get_secrets(secret_id):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_id)
    if 'SecretString' in response:
        secret = json.loads(response['SecretString'])

    return secret['password']


if __name__ == "__main__":
    pwd = get_secrets(str(sys.argv[1]))
    print(pwd)

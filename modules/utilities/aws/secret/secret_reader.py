import json

from modules.utilities.aws.boto_helper import get_boto_session


def get_secret(config):
    session = get_boto_session(config['region'], config['profile'])

    client = session.client(
        service_name='secretsmanager',
        region_name=config['region'],
    )

    get_secrets = client.get_secret_value(SecretId=config['vrdc_dev_arn'])

    if 'SecretString' in get_secrets:
        secret = get_secrets['SecretString']
    else:
        raise Exception('couldn\'t get the secret!')

    try:
        return json.loads(secret)
    except:
        return secret

import boto3

s3 = boto3.session.Session(profile_name="057921484265_SandboxAdministrator", region_name="us-east-1").client('s3')

def get_all_s3_keys(s3_path):
    """
    Get a list of all keys in an S3 bucket.

    :param s3_path: Path of S3 dir.
    """
    keys = []

    if not s3_path.startswith('s3://'):
        s3_path = 's3://' + s3_path

    bucket = s3_path.split('//')[1].split('/')[0]
    prefix = '/'.join(s3_path.split('//')[1].split('/')[1:])

    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys

if __name__ == "__main__":
    print(get_all_s3_keys("s3://snowflake-demo-etl/HSD_input_data/Data_to_DB"))
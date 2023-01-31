import io
from io import StringIO
from botocore.exceptions import ReadTimeoutError

from modules.utilities.aws.boto_helper import get_boto_session
from modules.utilities.generic.config_helper import load_raw_config


def write_s3(key, df, session=None):
    if session == None:
        config = load_raw_config()
        session = get_boto_session(config['region'], config['profile'])
    key = key.split(".")[0] + ".csv"
    s3 = session.client('s3')
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    body = buffer.getvalue()

    try:
        out_response = s3.put_object(
            Bucket=config['bucket'], Key=key, Body=body)
    except ReadTimeoutError:
        out_response = {"VersionId": "ReadTimeoutError"}
    return out_response

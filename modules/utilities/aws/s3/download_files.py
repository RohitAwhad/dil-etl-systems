import sys
import os

from modules.utilities.aws.boto_helper import get_boto_session
from modules.utilities.aws.s3.s3_read_helper import save_s3_body_to_csv, get_s3_keys
from modules.utilities.generic.config_helper import load_raw_config

from modules.utilities.generic.os_helper import clear_dir_contents, get_output_path


def download_files(config):
    bucket = config['bucket']
    session = get_boto_session(config['region'], config['profile'])

    output_path = get_output_path()
    clear_dir_contents(output_path)
    keys = get_s3_keys(session, bucket,config['prefix'])
    [save_s3_body_to_csv(session, bucket, key, output_path,config['prefix']) for key in keys]


if __name__ == "__main__":
    config = load_raw_config()
    bucket = str(sys.argv[1]) if len(sys.argv) > 1 else config['bucket']
    download_files(bucket)

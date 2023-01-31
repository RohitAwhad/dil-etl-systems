import boto3
import sys
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from configs.state_config import get_state_config
from modules.utilities.aws.s3.s3_read_helper import get_s3_keys
from modules.utilities.generic.config_helper import load_raw_config
from modules.utilities.generic.df_cleaner import load_csv_to_df
from sqlalchemy.dialects import registry
def remove_record_nulls(record):
    record_obj = {}
    for key in record.keys():
        if str(record[key]).lower() != 'nan':
            record_obj[key] = record[key]
    return record_obj

def insert_records_from_df(engine, model, df):
    try:
        session = sessionmaker(bind=engine)()
        connection = engine.connect()
        table_name = 'statetype'
        if_exists = 'append'
        df.to_sql(name=table_name.lower(), con=connection, if_exists=if_exists, index=False, chunksize=50000)
    finally:
        engine.dispose()

def load_snowflake_conn(awsconfig, dict):
    # session = boto3.session.Session(profile_name=awsconfig['profile'], region_name=awsconfig['region'],
    #                                 aws_access_key_id=awsconfig['aws_access_key_id'],
    #                                 aws_secret_access_key=awsconfig['aws_secret_access_key'],
    #                                 aws_session_token=awsconfig['aws_session_token'])
    #
    # keys = get_s3_keys(session, awsconfig['bucket'])
    # s3 = session.client('s3')
    #object = s3.get_object(Bucket=awsconfig['bucket'], Key=keys[6])
    #print(object['Body'].read())
    #with open(import_dir + "/rsa_key.p8", "rb") as key:
    with open("rsa_key.p8", "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    #print(dict.get('user'))
    # ctx = snowflake.connector.connect(
    #     user=dict.get('user'),
    #     account=dict.get('account'),
    #     private_key=pkb,
    #     warehouse=dict.get('warehouse'),
    #     database=dict.get('database'),
    #     schema=dict.get('schema'))

    #https://docs.snowflake.com/en/user-guide/sqlalchemy.html#key-pair-authentication-support
    # URL(
    #     account=dict.get('account'),
    #     user=dict.get('user'),
    #     warehouse="compute_wh",
    #     database="dw_poc",
    #     schema="public",
    #     role="SYSADMIN"
    #
    # ),
    engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse_name}&role={role_name}'.format(
        user=dict.get('user'),
        password='',
        account_identifier=dict.get('account'),
        database_name='dw_poc',
        schema_name='public',
        warehouse_name='compute_wh',
        role_name='SYSADMIN'
    ),
        connect_args={
            'private_key': pkb,
        },
    )

    return engine


def callMain():
    registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
    dict = {'user': 'rohit', 'account': 'hfqwdwx-wjb06002', 'warehouse': 'compute_wh', 'database': 'dw_poc',
            'schema': 'PUBLIC', 'role_name': 'accountadmin'}
    awsconfig = load_raw_config()
    engine = load_snowflake_conn(awsconfig, dict)
    session = sessionmaker(bind=engine)()
    print(engine)
    config = get_state_config()

    df_raw, col_dict = load_csv_to_df(
        config.raw_file_name, config.cols_to_rename)
    df_raw.columns = map(lambda x: str(x).upper(), df_raw.columns)
    dff = df_raw.loc[:, ['STATEABBREVIATION']]
    #insert_records_from_df(engine, config.main_model, dff)
    return "success"

if __name__ == "__main__":
    print(callMain())

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session
from sqlalchemy import create_engine

from modules.utilities.aws.secret.secret_reader import get_secret

def get_private_key_bytes(file):
    with open(file, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    return pkb
def get_sf_session(db,config):
    pkb = get_private_key_bytes(config['snowflake_privatekey'])
    snowpark_session = Session.builder\
        .config("account",config['snowflake_account'])\
        .config("user",config['snowflake_user'])\
        .config("role",config['snowflake_role'])\
        .config("database",db)\
        .config("warehouse",config['snowflake_warehouse'])\
        .config("schema",config['snowflake_schema'])\
        .config("private_key", pkb).create()
    return snowpark_session

def get_sf_engine(db,config):
    pkb = get_private_key_bytes(config['snowflake_privatekey'])
    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse_name}&role={role_name}'.format(
            user=config['snowflake_user'],
            password='',
            account_identifier=config['snowflake_account'],
            database_name=db,
            schema_name=config['snowflake_schema'],
            warehouse_name=config['snowflake_warehouse'],
            role_name=config['snowflake_role']
        ),
        connect_args={
            'private_key': pkb,
        },
    )

    return engine
def get_db_engine(db_name, config):
    db_secret = get_secret(config)
    return load_engine(db_secret, db_name)


def load_engine(db_secret, db_name):
    connection_url = \
        f"mysql+pymysql://{db_secret['username']}:{db_secret['password']}@{db_secret['host']}:{db_secret['port']}/{db_name}"

    engine = create_engine(connection_url, echo=False)
    return engine

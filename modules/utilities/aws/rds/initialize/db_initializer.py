import os
from modules.utilities.aws.rds.initialize.db_connector import get_sf_session, get_sf_engine
from modules.utilities.aws.rds.initialize.table_base import sqla_base
from modules.utilities.aws.rds.initialize.type_table_loader import load_type_tables
from modules.utilities.aws.rds.tables.table_register import *
from modules.utilities.aws.s3.download_files import download_files
from modules.utilities.generic.config_helper import load_raw_config


def initialize_db(overwrite=True, download=True):
    config = load_raw_config()
    db = config['db_name']
    engine = get_sf_engine(config['snowflake_database'],config)
    if download:
        download_files(config)

    if overwrite:
        reset_db(engine)
        load_type_tables(db, engine)

    return engine


def reset_db(engine):
    sqla_base.metadata.drop_all(bind=engine)
    sqla_base.metadata.create_all(bind=engine)


def reset_tables(engine):
    tables = sqla_base.metadata.sorted_tables
    [engine.execute(t.delete()) for t in tables]


if __name__ == "__main__":
    initialize_db()

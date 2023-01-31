import pandas as pd

from modules.utilities.aws.rds.initialize.db_connector import get_db_engine
from modules.utilities.generic.config_helper import load_raw_config


def get_table_as_df(connection, table_name):
    return pd.read_sql_table(table_name, connection)


if __name__ == "__main__":
    config = load_raw_config()
    engine = get_db_engine(config)
    df = get_table_as_df(engine, "rural_health_hospital_metrics")

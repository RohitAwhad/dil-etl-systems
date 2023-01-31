
from modules.utilities.aws.rds.create.row_inserter import load_table_from_df
from modules.utilities.aws.rds.query.foreign_key_helper import add_fks


def add_mean_assignments(engine, df, metric_config):
    df_w_fks = add_fks(engine, df, metric_config.fks)
    load_table_from_df(engine, metric_config.model, df_w_fks)

import pandas as pd
import re
from modules.utilities.aws.rds.create.row_inserter import load_table_from_df
from modules.utilities.aws.rds.query.foreign_key_helper import   add_fks

def get_df_melted(df, cols_to_keep):
    id_vars = [c for c in cols_to_keep if c in df.columns]
    df_melted = pd.melt(df, id_vars=id_vars, value_name='Value', var_name="MetricTypeName")
    df_no_null =  filter_out_nulls(df_melted)
    return df_no_null

def filter_out_nulls(df):
    df_numeric = df.loc[[
            is_number_regex(str(i)) for i in df['Value']]]
    
    df_no_null = df_numeric.loc[~pd.isnull(df_numeric['Value'])]
    return df_no_null

def is_number_regex(s):
    """ Returns True is string is a number. """
    if re.match("^\d+?\.\d+?$", s) is None:
        return s.isdigit()
    return True

def add_metric_assignments(engine, df, metric_config):
    df_filtered = df.loc[df["MetricTypeName"].isin(metric_config.cols)]
    df_w_fks = add_fks(engine, df_filtered, metric_config.fks)
    load_table_from_df(engine, metric_config.model, df_w_fks)


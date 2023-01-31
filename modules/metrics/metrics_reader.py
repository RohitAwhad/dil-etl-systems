import pandas as pd
from modules.utilities.aws.rds.create.row_inserter import load_table_from_df
from modules.utilities.aws.rds.tables.types.metric_type import MetricType
from modules.utilities.generic.os_helper import get_output_path

def add_metric_types(engine, df_raw, col_dict):
    df_metrics = get_df_metrics(df_raw, col_dict)
    add_metric_narrative_names(df_metrics)
    add_is_default_metric_type(df_metrics)
    add_is_excess_metric_type(df_metrics)
    load_table_from_df(engine, MetricType, df_metrics)



def get_df_metrics(df_raw, col_dict):
    df = pd.DataFrame()
    df['MetricTypeName'] = df_raw.columns
    df["RawName"] = df.apply(lambda row: get_metric_name(
        row['MetricTypeName'], col_dict), axis=1)
    return df


def get_metric_name(value, col_dict):
    return col_dict[value] if value in col_dict.keys() else value





def add_metric_narrative_names(df_metrics):
    names_df = pd.read_csv(get_output_path() + "types\\metric_type_names.csv")
    names_df['RawName'] = names_df['RawName'].str.upper()
    df_metrics['MetricDisplayName'] = df_metrics.apply(
        lambda row: get_metric_narrative_name(row, names_df), axis=1)


def get_metric_narrative_name(row, names_df):
    matches = names_df.loc[
        (names_df['RawName'] == row['RawName'].upper()),
        'MetricDisplayName'
    ]
    if len(matches) > 0:
        return matches.tolist()[0]
    else:
        return row['MetricTypeName']


def add_is_default_metric_type(df):
    default_metrics = ["SysBeds", "GrpCnt", "TotalMds"]
    df['IsDefaultMetricType'] = df.apply(
        lambda row: row['MetricTypeName'] in default_metrics, axis=1)


def add_is_excess_metric_type(df):
    df['IsExcessMetricType'] = df.apply(
        lambda row: 'excess' in row['MetricTypeName'].lower(), axis=1)


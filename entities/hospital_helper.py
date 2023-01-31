import pandas as pd
from modules.entities.county_helper import add_county_type_fk
from modules.utilities.aws.rds.tables.types.county_type import CountyType
from modules.utilities.generic import os_helper
from modules.metrics.metric_assignment_helper import filter_out_nulls
from modules.utilities.aws.rds.create.row_inserter import load_table_from_df
from modules.utilities.aws.rds.query.foreign_key_helper import  add_fk, add_fks



def clean_hospitals(df):
    df['HospitalName'] = df['HospitalName'].str.title()
    df_ccn_hsi_lookup = df[['CcnId', "Hsi"]]
    df_ccn_hsi_lookup.dropna(inplace=True)
    output_path = os_helper.get_path(
        "downloads\\rural\\2018") + "ccn_hsi_lookup.csv"
    df_ccn_hsi_lookup.to_csv(output_path, index=False)

def add_hospital_fks(engine, df, fks):
    for fk in fks:
        if fk.model == CountyType:
            df = add_county_type_fk(engine, df, fk)
        else:
            df = add_fk(engine, df, fk)
    return df


def add_system_median_assignments(engine, df, metric_config):
    df_filtered = df.loc[df["MetricTypeName"].isin(metric_config.cols)]
    df_filtered.MetricTypeName = df_filtered.MetricTypeName.str.replace("Median","")
    df_w_fks = add_fks(engine, df_filtered, metric_config.fks)
    df_w_fks.dropna(subset=["HealthSystemId"], inplace=True)
    df_w_fks.drop_duplicates(subset=['HealthSystemId', 'MetricTypeId'], keep='first', inplace=True)
    
    load_table_from_df(engine, metric_config.model, df_w_fks)

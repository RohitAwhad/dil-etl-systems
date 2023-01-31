import pandas as pd
import os
from modules.utilities.aws.rds.create.row_inserter import load_table_from_df
from modules.utilities.aws.rds.tables.types.county_type import CountyType
from modules.utilities.aws.rds.tables.types.ownership_type import OwnershipType
from modules.utilities.aws.rds.tables.types.state_type import StateType
from modules.utilities.generic.os_helper import get_output_path


def load_type_tables(db, engine):
    output_path = get_output_path() + "types/"
    if os.name == "nt":
        output_path = get_output_path() + "types\\"
    clean_state_county_file(db, output_path)
    load_table_from_df(engine, StateType, output_path + "state_types.csv")
    load_table_from_df(engine, CountyType, output_path + "county_types.csv")
    load_table_from_df(engine, OwnershipType,
                       output_path + "ownership_types.csv")


def clean_state_county_file(db, output_path):
    df = pd.read_csv(output_path + "us_state_county.csv")
    df_state = df[["CountyTypeName", "StateTypeId", "ZipCode"]]
    df_to_file(df_state, output_path + "county_types.csv")

    df_state = df[["StateTypeId", "StateTypeName", "StateAbbreviation"]]
    df_state["StateTypeName"] = df_state["StateTypeName"].str.title()
    df_to_file(df_state, output_path + "state_types.csv")


def df_to_file(df, file_name):
    df.drop_duplicates(inplace=True)
    df.to_csv(file_name)

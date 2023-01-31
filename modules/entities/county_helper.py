import pandas as pd
from sqlalchemy.orm import Session
from fuzzywuzzy import process

from modules.utilities.aws.rds.tables.types.county_type import CountyType
from modules.utilities.aws.rds.tables.types.state_type import StateType


def add_county_type_fk(engine, df, fk):
    print("Identifying county name best match...")
    county_type_dict = fk.custom_resource
    df_w_states = merge_df_on_states(engine, df)
    df_w_counties = get_df_w_counties(engine, df_w_states, county_type_dict)
    return df_w_counties


def get_df_w_counties(engine, df_w_states, county_type_dict):
    df_db_county = get_county_df_db(engine)
    df_w_counties = add_exact_county_matches(df_w_states, df_db_county)
    df_w_counties['CountyTypeId'] = df_w_counties.apply(
        lambda row: evaluate_has_county(row, df_db_county, county_type_dict), axis=1)
    return df_w_counties


def add_exact_county_matches(df_w_states, df_db_county):
    merge_cols = ['StateTypeId', 'CountyTypeName']
    return df_w_states.merge(df_db_county, how="left",
                             left_on=merge_cols, right_on=merge_cols)


def evaluate_has_county(row, df_db_county, county_type_dict):
    if not pd.isnull(row['CountyTypeId']):
        return row['CountyTypeId']
    else:
        return add_fuzzy_county_matches(row, df_db_county, county_type_dict)


def add_fuzzy_county_matches(row, df_db_county, county_type_dict):
    state_type_id = row['StateTypeId']
    current_name = row['CountyTypeName']
    city_name = row['City']
    if pd.isnull(current_name) and pd.isnull(city_name):
        return current_name
    elif pd.isnull(current_name) and not pd.isnull(city_name):
        Exception  # TODO: look up county via city name & state
    else:
        return get_fuzzy_row_match(current_name, state_type_id, df_db_county, county_type_dict)


def get_fuzzy_row_match(current_name, state_type_id, df_db_county, county_type_dict):
    try:
        # Known errors in provided data
        new_name = county_type_dict[state_type_id][current_name.lower()]
        return df_db_county.loc[
            (df_db_county['CountyTypeName'] == new_name) &
            (df_db_county['StateTypeId'] == state_type_id),
            'CountyTypeId'].values[0]
    except:
        return get_best_fuzzy_match(state_type_id, current_name, df_db_county)


def get_best_fuzzy_match(state_type_id, current_name, df_db_county):
    threshold = 80
    df_db_county_state = df_db_county.loc[df_db_county['StateTypeId']
                                          == state_type_id]
    best_match = process.extractOne(
        current_name, df_db_county_state['CountyTypeName'])

    if best_match != None and best_match[1] > threshold:
        new_name = best_match[0]
        new_type_id = df_db_county_state.loc[
            df_db_county_state['CountyTypeName'] == new_name,
            'CountyTypeId'].values[0]
        return new_type_id
    else:
        Exception


def get_county_df_db(engine):
    session = Session(engine)
    query = session.query(
        CountyType.CountyTypeId,
        CountyType.StateTypeId,
        CountyType.CountyTypeName)
    df_db = pd.read_sql(query.statement, query.session.bind)
    session.close()
    return df_db


def merge_df_on_states(engine, df):
    session = Session(engine)
    query = session.query(
        StateType.StateTypeId,
        StateType.StateAbbreviation)
    df_db = pd.read_sql(query.statement, query.session.bind)
    session.close()
    df_merged = df.merge(df_db, left_on="StateAbbreviation",
                         right_on="StateAbbreviation", how="left")
    df_merged.sort_values(by=['StateTypeId', 'CountyTypeName'])
    return df_merged

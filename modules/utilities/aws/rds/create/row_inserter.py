import pandas as pd
from sqlalchemy.orm import Session

from modules.utilities.aws.rds.query.model_helper import get_primary_key, get_df_model_cols_only, get_unique_constraint_cols, get_table_name_from_model
from modules.utilities.aws.rds.query.query_helper import get_table_as_df, get_df_db_with_natural_keys


def load_table_from_df(engine, model, df_to_load):
    if type(df_to_load) == str:
        df = pd.read_csv(df_to_load)
    else:
        df = get_df_model_cols_only(model, df_to_load)
    df_db = get_table_as_df(engine, model)
    df_model = prep_df_for_loading(engine, model, df, df_db)
    insert_records_from_df(engine, model, df_model)


def insert_records_from_df(engine, model, df):
    session = Session(engine)
    records = df.where(df.notnull(), None).to_dict('records')
    records_cleaned = list(
        map(lambda record: remove_record_nulls(record), records))
    session.bulk_insert_mappings(model, records_cleaned)
    session.commit()
    session.close()


def prep_df_for_loading(engine, model, df, df_db):
    print(f'Prepping model {get_table_name_from_model(model)} for loading')
    uc_col_sets = list(
        map(lambda x: x['column_names'], get_unique_constraint_cols(engine, model)))

    df_model = get_df_model_cols_only(model, df)
    df_model.drop_duplicates(inplace=True)

    df_no_internal_dupes = remove_internal_uc_dupes(df_model, uc_col_sets)
    df_no_db_dupes = remove_overlaps_w_db(
        df_no_internal_dupes, df_db, uc_col_sets)
    return df_no_db_dupes


def remove_internal_uc_dupes(df, uc_col_sets):
    row_cnt_0 = df.shape[0]
    for uc_cols in uc_col_sets:
        cols = [c for c in uc_cols if c in df.columns]
        df = df.drop_duplicates(subset=cols, keep='last')
    row_cnt_1 = df.shape[0]
    assert(row_cnt_0 == row_cnt_1)
    return df


def remove_overlaps_w_db(df, df_db, uc_col_sets):
    if df_db.shape[0] > 0:
        df['is_dupe'] = df.apply(lambda row: violates_uc_constraint(
            df_db, row, uc_col_sets), axis=1)
        return df.loc[~df['is_dupe']]
    else:
        return df


def violates_uc_constraint(df_compare, row, uc_col_sets):
    results = []
    for all_uc_cols in uc_col_sets:
        uc_cols = [c for c in all_uc_cols if c in row.index.values]
        overlap = [c for c in uc_cols if row[c] in df_compare[c].tolist()]
        result = len(overlap) == len(uc_cols)
        results.append(result)
    return len(list(filter(lambda x: x, results))) > 0


def load_add_and_update(engine, model, df_model, ref_cols=[]):
    records_add, records_update = prep_for_db(
        engine, model, ref_cols, df_model)
    send_to_db(engine, model, records_add, records_update)


def prep_for_db(engine, model, ref_cols, df):
    pk_col = get_primary_key(model)
    df_w_pk = add_db_pk_if_exists(engine, model, df, pk_col, ref_cols)
    df_add, df_update = split_df_by_update_type(df_w_pk, pk_col)

    return clean_records(df_add), clean_records(df_update)


def clean_records(df):
    records = df.to_dict('records')
    records_clean = [remove_record_nulls(record) for record in records]
    return records_clean


def add_db_pk_if_exists(engine, model, df_model, pk_col, ref_cols):
    df_db = get_df_db_with_natural_keys(engine, model, ref_cols)
    df_model[pk_col] = df_model.apply(
        lambda row: get_pk_id(row, df_db, ref_cols, pk_col), axis=1)
    return df_model


def get_pk_id(row, df_db, ref_cols, pk_col):
    for ref_col in ref_cols:
        df_ref_match = df_db.loc[df_db[ref_col] == row[ref_col]]
        if df_ref_match.shape[0] > 0:
            return df_ref_match[pk_col].values[0]
    return None


def send_to_db(engine, model, records_add, records_update):
    session = Session(engine)

    session.bulk_insert_mappings(model, records_add)
    session.commit()

    session.bulk_update_mappings(model, records_update)
    session.commit()

    session.close()


def split_df_by_update_type(df, pk_col):
    df_add = df.loc[pd.isnull(df[pk_col])]
    df_update = df.loc[~pd.isnull(df[pk_col])]
    return df_add, df_update


def remove_record_nulls(record):
    record_obj = {}
    for key in record.keys():
        if str(record[key]).lower() != 'nan':
            record_obj[key] = record[key]
    return record_obj

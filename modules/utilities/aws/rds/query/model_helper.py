from sqlalchemy import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.inspection import inspect
from modules.utilities.aws.rds.initialize.table_base import sqla_base


def get_table_name_from_model(model):
    return model.__table__.name


def get_df_model_cols_only(model, df):
    model_cols = [
        c for c in df.columns if c in get_column_names_from_model(model)]
    return df[model_cols]


def get_column_names_from_model(model):
    inst = inspect(model)
    return [c_attr.key for c_attr in inst.mapper.column_attrs]


def get_unique_constraint_cols(engine, model):
    insp = Inspector.from_engine(engine)
    uc = insp.get_unique_constraints(model.__table__.name)
    uc_col_sets = [i for i in uc if i['name'].startswith("uc__")]
    return uc_col_sets


def get_primary_key(model):
    return inspect(model).primary_key[0].name


def get_model_type_name(model):
    pk = inspect(model).primary_key[0].name
    return pk.replace("Id", "Name")


def get_pk_id_from_record(model, db_record):
    db_pk = get_primary_key(model)
    return getattr(db_record, db_pk)


def filter_df_numeric_only(model, df_raw):
    cols_to_drop = [c for c in df_raw.columns if
                    (df_raw[c].dtype == object) or
                    (c in get_column_names_from_model(model))
                    ]
    return df_raw.drop(columns=cols_to_drop)


def get_model_by_table_name(table_name):
    return list(filter(lambda t: t.name == table_name, sqla_base.metadata.sorted_tables))[0]

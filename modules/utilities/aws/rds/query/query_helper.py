import pandas as pd
from sqlalchemy.orm import Session

from modules.utilities.aws.rds.query.model_helper import get_primary_key


def get_query_as_df(query):
    return pd.read_sql(query.statement, query.session.bind)


def get_table_as_df(engine, model):
    session = Session(engine)
    query = session.query(model)
    df = pd.read_sql(query.statement, query.session.bind)
    session.close()
    return df


def get_record_from_db(session, filters, model):
    try:
        return session.query(model).filter_by(**filters).first()
    except:
        return None



def get_df_db_with_natural_keys(engine, model, natural_key_cols):
    session = Session(engine)
    pk_col = get_primary_key(model)
    ref_attrs = [getattr(model, i) for i in natural_key_cols]
    query = session.query(getattr(model, pk_col), *ref_attrs)
    df_db = pd.read_sql(query.statement, query.session.bind)
    session.close()
    return df_db

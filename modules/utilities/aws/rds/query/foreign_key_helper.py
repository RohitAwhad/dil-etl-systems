from classes.foreign_key_class import ForeignKey
from modules.utilities.aws.rds.query.query_helper import get_df_db_with_natural_keys


def add_fks(engine, df, fks):
    for fk in fks:
        df = add_fk(engine, df, fk)
    return df

def add_fk(engine, df, fk: ForeignKey): 
    df_foreign = get_df_db_with_natural_keys(engine, fk.model, fk.natural_keys)
    df_merged = df.merge(df_foreign, left_on=fk.natural_keys,
                      right_on=fk.natural_keys, how="left")
    return df_merged


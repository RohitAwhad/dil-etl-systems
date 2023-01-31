from modules.utilities.aws.rds.tables.types.county_type import CountyType
from modules.entities.county_helper import add_county_type_fk



def get_custom_merge(engine, df, fk):
    if fk.model == CountyType:
        df = add_county_type_fk(engine, df, fk)
    return df


from snowflake.snowpark.session import Session
import snowflake.snowpark.types as T
import snowflake.snowpark
from sqlalchemy import create_engine
import pandas as pd
import modules
from configs.state_config import get_state_config
from modules.utilities.aws.boto_helper import get_boto_session
from modules.utilities.aws.rds.initialize.db_connector import get_sf_session
from modules.utilities.aws.rds.initialize.db_initializer import reset_db
from modules.utilities.aws.s3.s3_read_helper import get_s3_keys, save_s3_body_to_csv
from modules.utilities.generic.config_helper import load_raw_config
from modules.utilities.generic.df_cleaner import load_csv_to_df
from modules.utilities.aws.rds.initialize.table_base import sqla_base
from modules.utilities.aws.rds.tables.table_register import *
from io import BytesIO
sconfig = load_raw_config()
snowpark_session = get_sf_session(sconfig['snowflake_database'],sconfig)

snowpark_session.add_packages('faker','asn1crypto','pandas','cryptography','sqlalchemy','requests','boto3','botocore','pyyaml','numpy','snowflake-snowpark-python')
snowpark_session.add_import('@ETL_S3_STAGE/dat_state_final.csv')
snowpark_session.add_import('@ETL_S3_STAGE/configs.zip')
snowpark_session.add_import('@ETL_S3_STAGE/constants.zip')
#snowpark_session.add_import('@ETL_S3_STAGE/modules.zip')
snowpark_session.add_import('@ETL_S3_STAGE/classes.zip')
snowpark_session.add_import('@ETL_S3_STAGE/config.yml')
def load_table_from_df_sf(SFsession, model, df_to_load):
    return ""

def load_type_tables_sf(db, SFsession):
    output_path = get_output_path_sf()+ "types/"
    df = pd.read_csv(output_path + "us_state_county.csv")
    df_state = df[["CountyTypeName", "StateTypeId", "ZipCode"]]
    df_state.drop_duplicates(inplace=True)
    dff = SFsession.create_dataframe(df_state)
    dff.write.copy_into_location("@ETL_S3_STAGE/"+output_path +"state_types.csv",file_format_type="csv", header=True, overwrite=True, single=True)

    df_state = df[["StateTypeId", "StateTypeName", "StateAbbreviation"]]
    df_state["StateTypeName"] = df_state["StateTypeName"].str.title()
    df_state.drop_duplicates(inplace=True)
    dff = SFsession.create_dataframe(df_state)
    dff.write.copy_into_location("@ETL_S3_STAGE/" + output_path +"county_types.csv", file_format_type="csv", header=True,
                                 overwrite=True, single=True)

    load_table_from_df_sf(SFsession, StateType, output_path + "state_types.csv")
    load_table_from_df_sf(SFsession, CountyType, output_path + "county_types.csv")
    load_table_from_df_sf(SFsession, OwnershipType,
                       output_path + "ownership_types.csv")

def get_output_path_sf():
    dir = "data/downloads/"
    return dir
def read_s3_body(session, bucket, key):
    s3 = session.client('s3')
    object = s3.get_object(Bucket=bucket, Key=key)
    return object['Body'].read()

def read_body_w_special_chars(body):
    try:
        bytes_str = BytesIO(body).read().decode('UTF-8')
    except:
        bytes_str = BytesIO(body).read().decode('ISO-8859-1')
    cleaned_lines = []
    lines = bytes_str.split("\r")
    col_count = len(lines[0].split(","))
    for line in lines:
        cells = line.replace("\n", "").split(",")
        if len(cells) > col_count:
            cells = [cells[0], cells[1] + cells[2], cells[3]]
        cleaned_lines += [cells]
    header = cleaned_lines[0]
    body = cleaned_lines[1:]
    return pd.DataFrame(body, columns=header)

def read_s3_body_to_df(session, read_bucket, key):
    body = read_s3_body(session, read_bucket, key)
    try:
        return pd.read_csv(BytesIO(body), index_col=None, engine='python')
    except:
        return read_body_w_special_chars(body)

def download_files_sf(config,SFsession):
    bucket = config['bucket']
    output_path = get_output_path_sf()
    session = get_boto_session(config['region'], config['profile'])
    keys = get_s3_keys(session, bucket,config['prefix'])
    [save_s3_body_to_csv_sf(session, SFsession,bucket, key,output_path,config['prefix']) for key in keys]

def save_s3_body_to_csv_sf(session,SFsession:Session, read_bucket, key, output_dir,prefix):
    df = read_s3_body_to_df(session, read_bucket, key)
    path = key.replace(prefix, "")
    dff=SFsession.create_dataframe(df)
    dff.write.copy_into_location("@ETL_S3_STAGE/"+output_dir+ path,file_format_type="csv", header=True, overwrite=True, single=True)


def initialize_sf(SFsession: snowflake.snowpark.session.Session,overwrite=True, download=True):
    config = load_raw_config()
    db = config['db_name']
    if download:
        download_files_sf(config,SFsession)

    if overwrite:
        reset_db(SFsession)
        load_type_tables_sf(db, SFsession)

def reset_db(SFsession: snowflake.snowpark.session.Session):
    SFsession.sql('''drop table if exists DW_POC.PUBLIC."CountyType";''').show()
    SFsession.sql('''drop table if exists DW_POC.PUBLIC."StateType";''').show()
    SFsession.sql('''drop table if exists DW_POC.PUBLIC."OwnershipType";''').show()
    SFsession.sql('''create or replace TABLE DW_POC.PUBLIC."StateType" (
        	"StateTypeId" NUMBER(38,0) NOT NULL autoincrement,
        	"StateTypeName" VARCHAR(50),
        	"StateAbbreviation" VARCHAR(2),
        	constraint uc__StateType__StateTypeName__StateAbbreviation unique ("StateTypeName", "StateAbbreviation"),
        	unique ("StateTypeName"),
        	unique ("StateAbbreviation"),
        	primary key ("StateTypeId")
        );''').show()
    SFsession.sql('''create or replace TABLE DW_POC.PUBLIC."OwnershipType" (
        	"OwnershipTypeId" NUMBER(38,0) NOT NULL autoincrement,
        	"OwnershipTypeName" VARCHAR(30),
        	constraint uc__OwnershipType__OwnershipTypeName unique ("OwnershipTypeName"),
        	primary key ("OwnershipTypeId")
        );''').show()
    SFsession.sql('''create or replace TABLE DW_POC.PUBLIC."CountyType" (
	"CountyTypeId" NUMBER(38,0) NOT NULL autoincrement,
	"CountyTypeName" VARCHAR(50),
	"StateTypeId" NUMBER(38,0),
	constraint UC__COUNTYTYPE__COUNTYTYPENAME__STATETYPEID unique ("CountyTypeName", "StateTypeId"),
	primary key ("CountyTypeId")
);''').show()


#initialize_sf(snowpark_session,True,False)
# def importCSVPROC(snowpark_session: snowflake.snowpark.session.Session):
#     config = get_state_config()
#     df_raw, col_dict = load_csv_to_df(
#         config.raw_file_name, config.cols_to_rename)
#     df_raw.columns = map(lambda x: str(x).upper(), df_raw.columns)
#     #dff = df_raw.loc[:, ['STATEABBREVIATION']]
#     snowpark_session.write_pandas(df=df_raw,
#             table_name='STATETYPE',
#             database='DW_POC',
#             schema='PUBLIC')#,auto_create_table=True
#     return f"inserted {len(df_raw.index)} into STATETYPE"
#
snowpark_session.sproc.register(
    func = initialize_sf
  , return_type = T.StringType()
  , input_types = [T.BooleanType,T.BooleanType]
  , is_permanent = True
  , name = 'SNOWPARK_GENERATE_CSVPROC'
  , replace = True
  , stage_location = '@ETL_PROC_STAGE'
)


def callMain():

  # snowpark_session.sql('''
  #   CALL SNOWPARK_GENERATE_CSVPROC()
  # ''').show()
  return "success"

if __name__ == "__main__":
    print(callMain())
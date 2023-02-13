import string

from snowflake.snowpark.session import Session
import snowflake.snowpark.types as T
import snowflake.snowpark
import pandas as pd
import os
from configs.state_config import get_state_config
from modules.utilities.aws.rds.initialize.db_connector import get_sf_session
from modules.utilities.generic.config_helper import load_raw_config
from modules.utilities.generic.df_cleaner import load_csv_to_df
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, FloatType

sconfig = load_raw_config()
snowpark_session = get_sf_session(sconfig['snowflake_database'],sconfig)
snowpark_session.add_packages('faker','asn1crypto','pandas','cryptography','sqlalchemy','requests','boto3','botocore','pyyaml','numpy','snowflake-snowpark-python')

def get_output_path_sf():
    dir = "data/downloads"
    return dir
def resetDB(snowpark_session: snowflake.snowpark.session.Session):
    snowpark_session.sql('''drop table if exists DW_POC.PUBLIC."CountyType";''').collect()
    snowpark_session.sql('''drop table if exists DW_POC.PUBLIC."StateType";''').collect()
    snowpark_session.sql('''drop table if exists DW_POC.PUBLIC."OwnershipType";''').collect()
    snowpark_session.sql('''create or replace TABLE DW_POC.PUBLIC."StateType" (
            	"StateTypeId" NUMBER(38,0) NOT NULL autoincrement,
            	"StateTypeName" VARCHAR(50),
            	"StateAbbreviation" VARCHAR(2)
            );''').collect()
    snowpark_session.sql('''create or replace TABLE DW_POC.PUBLIC."OwnershipType" (
            	"OwnershipTypeId" NUMBER(38,0) NOT NULL autoincrement,
            	"OwnershipTypeName" VARCHAR(30)
            );''').collect()
    snowpark_session.sql('''create or replace TABLE DW_POC.PUBLIC."CountyType" (
    	"CountyTypeName" VARCHAR(50),
    	"StateTypeId" NUMBER(38,0)
    );''').collect()
    output_path = get_output_path_sf() + "/types/"
    us_state_county_schema = StructType([StructField("StateTypeId", IntegerType()), StructField("StateTypeName", StringType()),
                              StructField("StateAbbreviation", StringType()), StructField("ZipCode", StringType()),
                              StructField("CountyTypeName", StringType()), StructField("City", StringType())])
    snowpark_session.sql("list @ETL_S3_STAGE/" +output_path+ "us_state_county.csv").show(max_width=1000)
    df = snowpark_session.read\
        .option("skip_header",1)\
        .option("field_delimiter",",")\
        .schema(us_state_county_schema)\
        .csv("@ETL_S3_STAGE/" +output_path+ "us_state_county.csv")

    df_state =df.select(["CountyTypeName", "StateTypeId"])
        #df[["CountyTypeName", "StateTypeId", "ZipCode"]]

    dff = df_state.drop_duplicates()
    print(dff.count)
    print(dff.show(max_width=1000))
    dff.write.mode("append").saveAsTable('''DW_POC.PUBLIC."CountyType"''')

    #
    # df_state = df[["StateTypeId", "StateTypeName", "StateAbbreviation"]]
    # df_state["StateTypeName"] = df_state["StateTypeName"].str.title()
    # df_state.drop_duplicates(inplace=True)
    # dff = snowpark_session.create_dataframe(df_state)
    # print(dff)




    # config = get_state_config()
    # df_raw, col_dict = load_csv_to_df(
    #     config.raw_file_name, config.cols_to_rename)
    # df_raw.columns = map(lambda x: str(x).upper(), df_raw.columns)
    # #dff = df_raw.loc[:, ['STATEABBREVIATION']]
    # snowpark_session.write_pandas(df=df_raw,
    #         table_name='STATETYPE',
    #         database='DW_POC',
    #         schema='PUBLIC')#,auto_create_table=True
#    dff.to_sql('customers', index=False, method=pd_writer)
    #return f"inserted  into STATETYPE" #len(df_raw.index)}

snowpark_session.sproc.register(
    func = resetDB
  , return_type = T.StringType()
  , input_types = []
  , is_permanent = True
  , name = 'SNOWPARK_GENERATE_CSVPROC'
  , replace = True
  , stage_location = '@ETL_S3_STAGE'
)


def callMain():
  snowpark_session.sql('''
    CALL SNOWPARK_GENERATE_CSVPROC()
  ''').show()
  #resetDB(snowpark_session)
  return "success"

if __name__ == "__main__":
    print(callMain())
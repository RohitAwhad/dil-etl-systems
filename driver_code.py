import string
from snowflake.snowpark.session import Session
import snowflake.connector
import snowflake.snowpark
from datetime import datetime
from snowflake.snowpark.functions import lit, call_udf
import configparser
from modules.utilities.aws.rds.initialize.db_connector import get_sf_session
from modules.utilities.generic.config_helper import load_raw_config
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, ArrayType,FloatType

from modules.utilities.snowflake.udf_function import concat_columns, padding_columns, hashkey_columns

sconfig = load_raw_config()
snowpark_session = get_sf_session(sconfig['snowflake_database'], sconfig)
snowpark_session.add_packages('faker', 'asn1crypto', 'pandas', 'cryptography', 'sqlalchemy', 'requests',     'boto3',
                              'botocore', 'pyyaml', 'numpy', 'snowflake-snowpark-python')


snowpark_session.add_import('@ETL_S3_STAGE/configs.zip')
snowpark_session.add_import('@ETL_S3_STAGE/constants.zip')
snowpark_session.add_import('@ETL_S3_STAGE/modules.zip')
snowpark_session.add_import('@ETL_S3_STAGE/classes.zip')
snowpark_session.add_import('@ETL_S3_STAGE/config.yml')

# Method to read config file settings
def read_config():
    config = configparser.ConfigParser()
    config.read('configurations.ini')
    return config

config = read_config()

USER = config['SNOWFLAKE_SETTINGS']['USER']
PASSWORD = config['SNOWFLAKE_SETTINGS']['PASSWORD']
ACCOUNT = config['SNOWFLAKE_SETTINGS']['ACCOUNT']
ROLE = config['SNOWFLAKE_SETTINGS']['ROLE']
WAREHOUSE = config['SNOWFLAKE_SETTINGS']['WAREHOUSE']
DATABASE = config['SNOWFLAKE_SETTINGS']['DATABASE']
SCHEMA = config['SNOWFLAKE_SETTINGS']['SCHEMA']

conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    role=ROLE,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

cs = conn.cursor()

def get_output_path_sf():
    dir = "data/downloads"
    return dir



def resetDB(snowpark_session: snowflake.snowpark.session.Session):
    snowpark_session.sql('''drop table if exists DW_POC.PUBLIC."CountyType";''').collect()
    snowpark_session.sql('''drop table if exists DW_POC.PUBLIC."OwnershipType";''').collect()
    snowpark_session.sql('''create or replace TABLE DW_POC.PUBLIC."OwnershipType" (
            	"OwnershipTypeId" NUMBER(38,0) NOT NULL autoincrement,
            	"OwnershipTypeName" VARCHAR(30),
                "OwnershipModifiedDate" TIMESTAMP
            );''').collect()
    snowpark_session.sql('''create or replace TABLE DW_POC.PUBLIC."CountyType" (
    	"CountyTypeName" VARCHAR(50),
    	"StateTypeId" NUMBER(38,0),
        "StateAbbreviation" VARCHAR(5),
        "ZipCode" VARCHAR(10)
    );''').collect()

    # for loading countytype table from s3 csv through snowpark api.
    output_path = get_output_path_sf() + "/types/"
    us_state_county_schema = StructType(
        [StructField("StateTypeId", IntegerType()), StructField("StateTypeName", StringType()),
         StructField("StateAbbreviation", StringType()), StructField("ZipCode", StringType()),
         StructField("CountyTypeName", StringType()), StructField("City", StringType())])
    snowpark_session.sql("list @ETL_S3_STAGE/" + output_path + "us_state_county.csv").show(max_width=1000)
    df = snowpark_session.read \
        .option("skip_header", 1) \
        .option("field_delimiter", ",") \
        .schema(us_state_county_schema) \
        .csv("@ETL_S3_STAGE/" + output_path + "us_state_county.csv")

    df_state = df.select(["CountyTypeName", "StateTypeId", "StateAbbreviation", "ZipCode"])
    # df[["CountyTypeName", "StateTypeId", "ZipCode"]]

    dff = df_state.drop_duplicates()
    print(dff.count)
    print(dff.show(max_width=1000))
    dff.write.mode("append").saveAsTable('''DW_POC.PUBLIC."CountyType"''')

    # for loading OwnershipType table from s3 csv through snowpark api.
    output_path = get_output_path_sf() + "/types/"
    ownership_types_schema = StructType(
        [StructField("OwnershipTypeId", IntegerType()), StructField("OwnershipTypeName", StringType())])
    snowpark_session.sql("list @ETL_S3_STAGE/" + output_path + "ownership_types.csv").show(max_width=1000)
    df = snowpark_session.read \
        .option("skip_header", 1) \
        .option("field_delimiter", ",") \
        .schema(ownership_types_schema) \
        .csv("@ETL_S3_STAGE/" + output_path + "ownership_types.csv")

    df = df.with_column('OwnershipModifiedDate', lit(datetime.now()))
    df_ownership = df.select(["OwnershipTypeId", "OwnershipTypeName", "OwnershipModifiedDate"])

    dff = df_ownership.drop_duplicates()
    print(dff.count)
    print(dff.show(max_width=1000))
    dff.write.mode("append").saveAsTable('''DW_POC.PUBLIC."OwnershipType"''')

## Register snowpark procedure in Snowflake
snowpark_session.sproc.register(
    func=resetDB
    , return_type=StringType()
    , input_types=[]
    , is_permanent=True
    , name='SNOWPARK_GENERATE_CSVPROC'
    , replace=True
    , stage_location='@ETL_S3_STAGE'
)

## Register UDF in Snowflake
snowpark_session.udf.register(
    func=concat_columns
    , return_type=StringType()
    , input_types=[StringType(),StringType()]
    , is_permanent=True
    , name='concat_columns'
    , replace=True
    , stage_location='@ETL_S3_STAGE'
)

snowpark_session.udf.register(
    func=padding_columns
    , return_type=StringType()
    , input_types=[StringType()]
    , is_permanent=True
    , name='padding_columns'
    , replace=True
    , stage_location='@ETL_S3_STAGE'
)

snowpark_session.udf.register(
    func=hashkey_columns
    , return_type=StringType()
    , input_types=[StringType(), StringType(), StringType(), StringType()]
    , is_permanent=True
    , name='hashkey_columns'
    , replace=True
    , stage_location='@ETL_S3_STAGE'
)

def callMain():
    cs.execute("CALL SNOWPARK_GENERATE_CSVPROC()")
    cs.execute("CALL CREATE_TABLE_USING_STORED_PROCEDURE()")
    cs.close()
    return "process executed successfully.."

if __name__ == "__main__":
    print(callMain())

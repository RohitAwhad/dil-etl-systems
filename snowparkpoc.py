
from snowflake.snowpark.session import Session
import snowflake.snowpark.types as T
import snowflake.snowpark
from configs.state_config import get_state_config
from modules.utilities.aws.rds.initialize.db_connector import get_sf_session
from modules.utilities.generic.config_helper import load_raw_config
from modules.utilities.generic.df_cleaner import load_csv_to_df
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# with open("rsa_key.p8", "rb") as key:
#     p_key = serialization.load_pem_private_key(
#         key.read(),
#         password=None,
#         backend=default_backend()
#     )
#
# pkb = p_key.private_bytes(
#     encoding=serialization.Encoding.DER,
#     format=serialization.PrivateFormat.PKCS8,
#     encryption_algorithm=serialization.NoEncryption())
#
# connection_parameters = {
#     "account": "hfqwdwx-wjb06002",
#     "user": "rohit",
#     "role": "SYSADMIN",
#     "warehouse": "compute_wh",
#     "database": "dw_poc",
#     "schema": "public"
#   }

sconfig = load_raw_config()
snowpark_session = get_sf_session(sconfig['snowflake_database'],sconfig)
#'PyMySQL','pyinstaller'
snowpark_session.add_packages('faker','asn1crypto','pandas','cryptography','sqlalchemy','requests','boto3','botocore','pyyaml','numpy','snowflake-snowpark-python')
snowpark_session.add_import('@ETL_S3_STAGE/dat_state_final.csv')
snowpark_session.add_import('@ETL_S3_STAGE/configs.zip')
snowpark_session.add_import('@ETL_S3_STAGE/constants.zip')
snowpark_session.add_import('@ETL_S3_STAGE/modules.zip')
snowpark_session.add_import('@ETL_S3_STAGE/classes.zip')
# snowpark_session.add_import('@ETL_S3_STAGE/config.yml')
#'@ETL_S3_STAGE/constants.zip','@ETL_S3_STAGE/modules.zip','@ETL_S3_STAGE/classes.zip','@ETL_S3_STAGE/tests.zip','@ETL_S3_STAGE/config.yml'
def importCSVPROC(snowpark_session: snowflake.snowpark.session.Session):
    config = get_state_config()
    df_raw, col_dict = load_csv_to_df(
        config.raw_file_name, config.cols_to_rename)
    df_raw.columns = map(lambda x: str(x).upper(), df_raw.columns)
    #dff = df_raw.loc[:, ['STATEABBREVIATION']]
    snowpark_session.write_pandas(df=df_raw,
            table_name='STATETYPE',
            database='DW_POC',
            schema='PUBLIC')#,auto_create_table=True
    return f"inserted {len(df_raw.index)} into STATETYPE"

snowpark_session.sproc.register(
    func = importCSVPROC
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
  return "success"

if __name__ == "__main__":
    print(callMain())
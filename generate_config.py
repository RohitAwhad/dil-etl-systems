import configparser

# CREATE OBJECT
config_file = configparser.ConfigParser()

# ADD SECTION
config_file.add_section("SNOWFLAKE_SETTINGS")
# ADD SETTINGS TO SECTION
config_file.set("SNOWFLAKE_SETTINGS", "USER", "vikasa")
config_file.set("SNOWFLAKE_SETTINGS", "PASSWORD", "lygx]0mgJ#250791$")
config_file.set("SNOWFLAKE_SETTINGS", "ACCOUNT", "hfqwdwx-wjb06002")
config_file.set("SNOWFLAKE_SETTINGS", "ROLE", "ACCOUNTADMIN")
config_file.set("SNOWFLAKE_SETTINGS", "WAREHOUSE", "COMPUTE_WH")
config_file.set("SNOWFLAKE_SETTINGS", "DATABASE", "DW_POC")
config_file.set("SNOWFLAKE_SETTINGS", "SCHEMA", "PUBLIC")
# SAVE CONFIG FILE
with open(r"configurations.ini", 'w') as configfileObj:
    config_file.write(configfileObj)
    configfileObj.flush()
    configfileObj.close()

print("Config file 'configurations.ini' created")

# PRINT FILE CONTENT
read_file = open("configurations.ini", "r")
content = read_file.read()
print("Content of the config file are:\n")
print(content)
read_file.flush()
read_file.close()
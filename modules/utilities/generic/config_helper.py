import os
import yaml

def is_running_local():
    if 'USERDNSDOMAIN' in os.environ.keys():
        return os.environ['USERDNSDOMAIN'] == 'MATHEMATICA.NET'
    else:
        return False

def load_raw_config():
    if is_running_local():
        file_path_name="\\config.yml"
    else:
        file_path_name = "/config.yml"
    project_dir = os.getcwd()
    config_file = project_dir + file_path_name
    with open(config_file, "r") as stream:
        raw_config = yaml.safe_load(stream)
    return raw_config


    

    

if __name__ == "__main__":
    load_raw_config()
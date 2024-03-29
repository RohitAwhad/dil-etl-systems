import os
import shutil

def clear_dir_contents(dir):
    if os.path.isdir(dir):
        shutil.rmtree(dir)
    os.mkdir(dir)
    
def delete_file(path):
    os.remove(path)
    
def get_path(dir):
    tempDir =dir
    dir = os.getcwd() + f"/data/{tempDir}/"
    print(dir)
    if os.name == "nt":
        dir = ""
        dir = os.getcwd() + f"\\data\\{tempDir}\\"
    if not os.path.exists(dir):
        os.makedirs(dir)
    return dir
    
def get_output_path():
    dir = "downloads"
    return get_path(dir)

output_dir = get_output_path() 

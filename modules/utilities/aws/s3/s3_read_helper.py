import os
import pandas as pd
from io import BytesIO

def get_s3_keys(session, bucket, prefix=''):
    s3 = session.client('s3')
    keys = []
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    resp = s3.list_objects_v2(**kwargs)
    for obj in resp['Contents']:
        keys.append(obj['Key'])
    file_keys = [i for i in keys if not i.endswith("/")]
    return file_keys

def read_s3_body(session, bucket, key):
    s3 = session.client('s3')
    object = s3.get_object(Bucket=bucket, Key=key)
    return object['Body'].read()


def save_s3_body_to_csv(session, read_bucket, key, output_dir,prefix):
    path = output_dir + key.replace("/", "\\")
    if os.name == "nt":
        prefix = prefix.replace("/","\\")
        path = path.replace(prefix,"")
    parent_dirs = "\\".join(path.split("\\")[:-1])
    os.makedirs(parent_dirs, exist_ok=True)
    df = read_s3_body_to_df(session, read_bucket, key)
    df.to_csv(path, index=False)

def read_s3_body_to_df(session, read_bucket, key):
    body = read_s3_body(session, read_bucket, key)
    try: 
        return pd.read_csv(BytesIO(body), index_col=None, engine='python')
    except: 
        return read_body_w_special_chars(body)
        
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
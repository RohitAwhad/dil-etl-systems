import hashlib

#udf creation for transformationn.
def concat_columns(column1, column2):
    concat_col = str(column1) + "-" + str(column2)
    return concat_col

def padding_columns(column1):
    padding_col = str(column1).rjust(10, '0')
    return padding_col

def hashkey_columns(column1, column2, column3, column4):
    hashkey_col = hashlib.md5(str(column1).encode() + str(column2).encode() + str(column3).encode() + str(column4).encode())
    return hashkey_col.hexdigest()
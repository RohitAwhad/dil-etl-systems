import pandas as pd
import numpy as np
import re



def load_csv_to_df(raw_file_name, cols_to_rename):
    print(f"Extracting {raw_file_name}")
    df_raw = pd.read_csv(raw_file_name)
    raw_cols = df_raw.columns
    df_raw.columns = [clean_column_name(
        cols_to_rename, c) for c in df_raw.columns]
    new_cols = df_raw.columns
    col_dict = dict(zip(new_cols, raw_cols))
    clean_ccns(df_raw)
    return df_raw, col_dict


def clean_ccns(df):
    if 'CcnId' in df.columns:
        # Known errors in the data
        df['CcnId'] = df['CcnId'].replace("4.90E+132", "470012")
        df['CcnId'] = df['CcnId'].replace("5.10E+149", "51E148")


def clean_column_name(cols_to_rename, col_name):
    c_pascal = to_pascal_case(col_name)
    new_col_name = cols_to_rename[c_pascal] if c_pascal in cols_to_rename.keys(
    ) else c_pascal
    return new_col_name


def to_pascal_case(col):
    col_snake = col.replace(" ", "_")
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    col_case_sensitive = pattern.sub('_', col_snake).lower()
    col_split = col_case_sensitive.split("_")
    return "".join(list(map(lambda x: x.title(), col_split)))


def to_snake_case(col):
    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    col_snake = pattern.sub("_", col).lower()
    col_no_spaces = "".join((col_snake.split(" ")))
    return col_no_spaces


def remove_null_values(df):
    return df[df['Value'].notna()]


def add_geo_coords(df, loc_col):
    df['Latitude'] = df.apply(lambda row: parse_point(row[loc_col], 1), axis=1)
    df['Longitude'] = df.apply(
        lambda row: parse_point(row[loc_col], 0), axis=1)
    df.drop(columns=[loc_col], inplace=True)


def parse_point(value, position):
    if pd.isnull(value) or str(value) == "NaN":
        return None
    else:
        pat = r"POINT \((\S*) (\S*)\)"
        match = re.match(pat, value)
        return match[position + 1]


def clean_merge_cols(df_merged, df_x, df_y):
    shared_cols = [
        c for c in df_x.columns if c in df_y and c not in df_merged.columns]

    for col in shared_cols:
        col_x = col + "_x"
        col_y = col + "_y"
        df_merged[col] = np.where(~pd.isnull(
            df_merged[col_x]), df_merged[col_x], df_merged[col_y])
        df_merged.drop(columns=[col_x, col_y], inplace=True)


def sort_df_by_asc_null_count(df):
    return df.iloc[df.isnull().sum(1).sort_values(ascending=True).index]

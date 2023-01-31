def clean_systems(df):
    df['SysMaPlanContracts'] = df.apply(
        lambda row: count_contracts(row), axis=1)


def count_contracts(row):
    if str(row['SysMaPlanContracts']).lower() == 'nan':
        return 0
    else:
        return len(row['SysMaPlanContracts'].split(";"))


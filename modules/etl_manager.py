from configs.config_factory import get_config
from modules.utilities.aws.rds.query.foreign_key_helper import add_fks
from modules.metrics.metrics_reader import add_metric_types
from modules.metrics import metric_assignment_helper
from modules.utilities.aws.rds.create.row_inserter import load_table_from_df
from modules.utilities.generic.df_cleaner import load_csv_to_df
from constants.entity_enum import EntityTypeEnum
from modules.entities import system_helper
from modules.entities import hospital_helper
from modules.entities import state_helper


def run_health_systems(engine):
    load_health_systems(engine)
    load_hospitals(engine)
    load_states(engine)

def load_health_systems(engine):
    config = get_config(EntityTypeEnum.SYSTEM)
    df_raw, col_dict = load_csv_to_df(
        config.raw_file_name, config.cols_to_rename)
    system_helper.clean_systems(df_raw)
    df_w_fks = add_fks(engine, df_raw, config.foreign_keys)
    load_table_from_df(engine, config.main_model, df_w_fks)
    add_metric_types(engine, df_w_fks, col_dict)
    
    df_melted = metric_assignment_helper.get_df_melted(df_w_fks, cols_to_keep=config.natural_keys)
    metric_assignment_helper.add_metric_assignments(engine, df_melted, config.categorical_metrics)
    metric_assignment_helper.add_metric_assignments(engine, df_melted, config.continuous_metrics)



def load_hospitals(engine):
    config = get_config(EntityTypeEnum.HOSPITAL)
    df_raw, col_dict = load_csv_to_df(
        config.raw_file_name, config.cols_to_rename)
    hospital_helper.clean_hospitals(df_raw)
    df_w_fks = hospital_helper.add_hospital_fks(engine, df_raw, config.foreign_keys)
    load_table_from_df(engine, config.main_model, df_w_fks)

    df_no_medians = df_w_fks[[c for c in df_w_fks.columns if c not in config.median_metrics.cols]]
    add_metric_types(engine, df_no_medians, col_dict)

    df_melted = metric_assignment_helper.get_df_melted(df_w_fks, config.get_melt_cols())
    metric_assignment_helper.add_metric_assignments(engine, df_melted, config.categorical_metrics)
    metric_assignment_helper.add_metric_assignments(engine, df_melted, config.continuous_metrics)
    hospital_helper.add_system_median_assignments(engine, df_melted, config.median_metrics)

def load_states(engine):
    config = get_config(EntityTypeEnum.STATE)
    df_raw, col_dict = load_csv_to_df(
        config.raw_file_name, config.cols_to_rename)
    df_w_fks = add_fks(engine, df_raw, config.foreign_keys)
    load_table_from_df(engine, config.main_model, df_w_fks[df_w_fks['StateAbbreviation'] != 'National Avg'])
    add_metric_types(engine, df_w_fks, col_dict)

    df_melted = metric_assignment_helper.get_df_melted(df_w_fks, config.get_melt_cols())
    
    df_no_mean = df_melted.loc[df_melted['StateAbbreviation'] != "National Avg"]
    metric_assignment_helper.add_metric_assignments(engine, df_no_mean, config.continuous_metrics)

    df_mean = df_melted.loc[df_melted['StateAbbreviation'] == "National Avg"]
    state_helper.add_mean_assignments(engine, df_mean, config.mean_metrics)

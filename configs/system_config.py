from classes.entity_config_class import EntityConfig
from classes.foreign_key_class import ForeignKey
from classes.metric_column_class import MetricColumn
from modules.utilities.aws.rds.tables.types.ownership_type import OwnershipType
from modules.utilities.aws.rds.tables.types.state_type import StateType
from modules.utilities.aws.rds.tables.health_system import HealthSystem
from modules.utilities.aws.rds.tables.assignments.health_system_continuous_metric import HealthSystemContinuousMetricAssignment
from modules.utilities.aws.rds.tables.assignments.health_system_categorical_metric import HealthSystemCategoricalMetricAssignment
from constants.columns.systems_hsi_columns import hsi_hsi_cont_columns, hsi_hsi_cat_columns
from constants.columns.systems_columns_to_rename import systems_health_system_cols_to_rename


def get_system_config():
    config = EntityConfig()
    config.pk_col = "HealthSystemId"
    config.natural_keys = ["Hsi"]
    config.entity_type_name = 'health_system'
    config.main_model = HealthSystem
    config.table_name = "HealthSystem"
    config.raw_file_name = 'data/downloads/dat_health_system_final.csv'
    config.cols_to_rename = systems_health_system_cols_to_rename
    config.foreign_keys = [
        ForeignKey(StateType, ['StateAbbreviation']),
        ForeignKey(OwnershipType, ['OwnershipTypeName'])
    ]
    config.continuous_metrics = MetricColumn(HealthSystemContinuousMetricAssignment,
                                             hsi_hsi_cont_columns, [ForeignKey(config.main_model, config.natural_keys)])
    config.categorical_metrics = MetricColumn(HealthSystemCategoricalMetricAssignment,
                                              hsi_hsi_cat_columns, [ForeignKey(config.main_model, config.natural_keys)])
    return config

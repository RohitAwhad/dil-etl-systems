from classes.entity_config_class import EntityConfig
from classes.foreign_key_class import ForeignKey
from classes.metric_column_class import MetricColumn
from modules.utilities.aws.rds.tables.assignments.state_type_continuous_metric_assignment import StateTypeContinuousMetricAssignment
from modules.utilities.aws.rds.tables.assignments.national_mean import NationalMeanMetric
from constants.columns.systems_state_metric_columns import systems_state_metric_cont_cols
from constants.columns.systems_columns_to_rename import systems_state_cols_to_rename
from modules.utilities.aws.rds.tables.types.state_type import StateType


def get_state_config():
    config = EntityConfig()
    config.pk_col = "StateTypeId"
    config.natural_keys = ["StateAbbreviation"]
    config.entity_type_name = 'state'
    config.main_model = StateType
    config.table_name = "StateType"
    config.raw_file_name = 'data\\downloads\\dat_state_final.csv'
    config.cols_to_rename = systems_state_cols_to_rename
    config.foreign_keys = []
    config.continuous_metrics = MetricColumn(StateTypeContinuousMetricAssignment,
                                             systems_state_metric_cont_cols, [ForeignKey(config.main_model, config.natural_keys)])
    config.mean_metrics = MetricColumn(NationalMeanMetric,
                                              systems_state_metric_cont_cols, [ForeignKey(config.main_model, config.natural_keys)])
    return config

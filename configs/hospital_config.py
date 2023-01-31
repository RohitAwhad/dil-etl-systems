from classes.entity_config_class import EntityConfig
from classes.foreign_key_class import ForeignKey
from classes.metric_column_class import MetricColumn
from modules.utilities.aws.rds.tables.assignments.hospital_categorical_metric_assignment import HospitalCategoricalMetricAssignment
from modules.utilities.aws.rds.tables.assignments.hospital_continuous_metric_assignment import HospitalContinuousMetricAssignment
from modules.utilities.aws.rds.tables.assignments.hospital_metric_system_median import HospitalMetricSystemMedian
from modules.utilities.aws.rds.tables.health_system import HealthSystem
from modules.utilities.aws.rds.tables.hospital import Hospital
from constants.columns.systems_hospital_columns import hsi_hospital_cat_columns, hsi_hospital_cont_columns, hsi_hospital_median_columns
from constants.columns.systems_columns_to_rename import systems_hospital_cols_to_rename
from modules.utilities.aws.rds.tables.types.county_type import CountyType
from constants.county_dict import county_type_dict


def get_hospital_config():
    config = EntityConfig()
    config.pk_col = "HospitalId"
    config.natural_keys = ["CcnId", "CompendiumId"]
    config.entity_type_name = 'hospital'
    config.main_model = Hospital
    config.table_name = "Hospital"
    config.raw_file_name = 'data\\downloads\\dat_hospital_final.csv'
    config.cols_to_rename = systems_hospital_cols_to_rename
    config.foreign_keys = [
        ForeignKey(CountyType, ['CountyTypeName'], custom_resource=county_type_dict),
        ForeignKey(HealthSystem, ['Hsi'])
    ]
    config.continuous_metrics = MetricColumn(HospitalContinuousMetricAssignment,
                                             hsi_hospital_cont_columns, [ForeignKey(config.main_model, config.natural_keys)])
    config.categorical_metrics = MetricColumn(HospitalCategoricalMetricAssignment,
                                              hsi_hospital_cat_columns, [ForeignKey(config.main_model, config.natural_keys)])
    config.median_metrics = MetricColumn(HospitalMetricSystemMedian,
                                              hsi_hospital_median_columns, 
                                              [ForeignKey(config.main_model, config.natural_keys),
                                              ForeignKey(HealthSystem, ["HealthSystemId"])])
    return config

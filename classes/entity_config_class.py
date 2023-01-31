from classes.foreign_key_class import ForeignKey


class EntityConfig:
    def __init__(self):
        self.entity_type_name = None
        self.pk_col = None
        self.natural_keys = []
        self.main_model = None
        self.raw_file_name = None
        self.ref_cols = None
        self.cols_to_rename = None
        self.foreign_keys: list[ForeignKey] = []
        self.continuous_metrics = None
        self.categorical_metrics = None
        self.median_metrics = None
        self.mean_metrics = None

    def get_melt_cols(self):
        melt_cols = []
        for i in [self.continuous_metrics, self.categorical_metrics, self.median_metrics, self.mean_metrics]:
            if i != None:
                melt_cols += i.get_fk_natural_keys()
        return list(set(melt_cols))
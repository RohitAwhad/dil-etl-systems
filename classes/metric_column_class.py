from itertools import chain
from classes.foreign_key_class import ForeignKey
from modules.utilities.aws.rds.tables.types.metric_type import MetricType


class MetricColumn:
    def __init__(self, model, cols, fks):
        self.model = model
        self.cols = cols
        self.fks = fks + [ForeignKey(MetricType, ['MetricTypeName'])]
       
    def get_fk_natural_keys(self):
        return chain.from_iterable(list(map(lambda fk: fk.natural_keys, self.fks)))

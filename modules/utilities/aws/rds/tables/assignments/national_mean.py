from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import UniqueConstraint, ForeignKeyConstraint

from modules.utilities.aws.rds.initialize.table_base import sqla_base


class NationalMeanMetric(sqla_base):
    __tablename__ = "NationalMeanMetric"
    NationalMeanMetricId = Column(
        Integer, autoincrement=True, primary_key=True)
    Value = Column(Float, nullable=False)

    MetricTypeId = Column(Integer, nullable=False)
    MetricType = relationship("MetricType")
    
    __table_args__ = (
        UniqueConstraint(
            'MetricTypeId', name="uc__NationalMeanMetric__MetricTypeId"),
        ForeignKeyConstraint(['MetricTypeId'], [
                             'MetricType.MetricTypeId'], name="fk__NationalMeanMetric__MetricType"),
    )

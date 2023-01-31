from sqlalchemy import Column, Float, Integer, String,BOOLEAN
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import UniqueConstraint, ForeignKeyConstraint
from sqlalchemy.sql.sqltypes import SMALLINT

from modules.utilities.aws.rds.initialize.table_base import sqla_base


class MetricType(sqla_base):
    __tablename__ = "MetricType"
    MetricTypeId = Column(Integer, autoincrement=True, primary_key=True)
    MetricTypeName = Column(String(200))
    MetricDisplayName = Column(String(200))
    IsDefaultMetricType = Column(BOOLEAN)
    IsExcessMetricType = Column(BOOLEAN)

    HospitalContinuousMetrics = relationship(
        "HospitalContinuousMetricAssignment", back_populates="MetricType")
    HospitalCategoricalMetrics = relationship(
        "HospitalCategoricalMetricAssignment", back_populates="MetricType")

    HealthSystemContinuousMetrics = relationship(
        "HealthSystemContinuousMetricAssignment", back_populates="MetricType")
    HealthSystemCategoricalMetrics = relationship(
        "HealthSystemCategoricalMetricAssignment", back_populates="MetricType")

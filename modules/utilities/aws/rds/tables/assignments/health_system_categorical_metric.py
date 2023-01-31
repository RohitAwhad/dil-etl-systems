from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, SMALLINT

from sqlalchemy.sql.schema import ForeignKeyConstraint, UniqueConstraint
from modules.utilities.aws.rds.initialize.table_base import sqla_base


class HealthSystemCategoricalMetricAssignment(sqla_base):
    __tablename__ = "HealthSystemCategoricalMetricAssignment"
    HealthSystemCategoricalMetricAssignmentId = Column(
        Integer, autoincrement=True, primary_key=True)
    Value = Column(SMALLINT, nullable=False)

    MetricTypeId = Column(Integer, nullable=False)
    MetricType = relationship("MetricType")

    HealthSystemId = Column(Integer, nullable=False)
    HealthSystem = relationship("HealthSystem")

    __table_args__ = (
        ForeignKeyConstraint(['MetricTypeId'], ['MetricType.MetricTypeId'],
                             name="fk__HealthSystemMetricCat__MetricType__MetricTypeId"),
        ForeignKeyConstraint(['HealthSystemId'], [
                             'HealthSystem.HealthSystemId'], name="fk__HealthSystemMetricCat__HealthSystem"),
        UniqueConstraint('HealthSystemId', 'MetricTypeId',
                         name="uc__HealthSystemMetricCat__HealthSystemId__MetricTypeId"),
    )

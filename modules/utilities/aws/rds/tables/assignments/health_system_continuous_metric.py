from sqlalchemy import Column, Numeric, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKeyConstraint, UniqueConstraint

from modules.utilities.aws.rds.initialize.table_base import sqla_base


class HealthSystemContinuousMetricAssignment(sqla_base):
    __tablename__ = "HealthSystemContinuousMetricAssignment"
    HealthSystemContinuousMetricAssignmentId = Column(
        Integer, autoincrement=True, primary_key=True)
    Value = Column(Numeric(20, 4), nullable=False)

    MetricTypeId = Column(Integer, nullable=False)
    MetricType = relationship("MetricType")

    HealthSystemId = Column(Integer, nullable=False)
    HealthSystem = relationship("HealthSystem")

    __table_args__ = (
        ForeignKeyConstraint(['MetricTypeId'], ['MetricType.MetricTypeId'],
                             name="fk__HealthSystemMetricCont__MetricType__MetricTypeId"),
        ForeignKeyConstraint(['HealthSystemId'], ['HealthSystem.HealthSystemId'],
                             name="fk__HealthSystemMetricCont__HealthSystem__HealthSystemId"),
        UniqueConstraint('HealthSystemId', 'MetricTypeId',
                         name="uc__HealthSystemMetricCont__HealthSystemId__MetricTypeId"),
    )

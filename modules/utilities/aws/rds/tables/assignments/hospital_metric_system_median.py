from sqlalchemy import Column, Float, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import UniqueConstraint, ForeignKeyConstraint

from modules.utilities.aws.rds.initialize.table_base import sqla_base


class HospitalMetricSystemMedian(sqla_base):
    __tablename__ = "HospitalMetricSystemMedian"
    SystemMedianId = Column(Integer, autoincrement=True, primary_key=True)
    Value = Column(Float, nullable=False)

    MetricTypeId = Column(Integer, nullable=False)
    MetricType = relationship("MetricType")

    HealthSystemId = Column(Integer, nullable=True)
    HealthSystem = relationship("HealthSystem")

    __table_args__ = (
        UniqueConstraint('MetricTypeId', 'HealthSystemId',
                         name="uc__SystemMedian__MetricTypeId__HealthSystemId"),
        ForeignKeyConstraint(['MetricTypeId'], [
                             'MetricType.MetricTypeId'], name="fk__SystemMedian__MetricType"),
        ForeignKeyConstraint(['HealthSystemId'], ['HealthSystem.HealthSystemId'],
                             name="fk__SystemMedian__HealthSystem__HealthSystemId"),
    )

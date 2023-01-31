from sqlalchemy import Column, Numeric, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKeyConstraint, UniqueConstraint

from modules.utilities.aws.rds.initialize.table_base import sqla_base


class HospitalContinuousMetricAssignment(sqla_base):
    __tablename__ = "HospitalContinuousMetricAssignment"
    HospitalContinuousMetricAssignmentId = Column(
        Integer, autoincrement=True, primary_key=True)
    Value = Column(Numeric(20, 4), nullable=False)

    MetricTypeId = Column(Integer, nullable=False)
    MetricType = relationship("MetricType")

    HospitalId = Column(Integer, nullable=False)
    Hospital = relationship("Hospital")

    __table_args__ = (
        ForeignKeyConstraint(['MetricTypeId'], ['MetricType.MetricTypeId'],
                             name="fk__HospitalMetricCont__MetricType__MetricTypeId"),
        ForeignKeyConstraint(['HospitalId'], [
                             'Hospital.HospitalId'], name="fk__HospitalMetricCont__Hospital__HospitalId"),
        UniqueConstraint('HospitalId', 'MetricTypeId',
                         name="uc__HospitalMetricCont__HospitalId__MetricTypeId"),
    )

from datetime import datetime
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Float, String, DateTime
from sqlalchemy.sql.sqltypes import SMALLINT

from sqlalchemy.sql.schema import ForeignKeyConstraint, UniqueConstraint
from modules.utilities.aws.rds.initialize.table_base import sqla_base


class HospitalCategoricalMetricAssignment(sqla_base):
    __tablename__ = "HospitalCategoricalMetricAssignment"
    HospitalCategoricalMetricId = Column(
        Integer, autoincrement=True, primary_key=True)
    Value = Column(SMALLINT, nullable=False)

    MetricTypeId = Column(Integer, nullable=False)
    MetricType = relationship("MetricType")

    HospitalId = Column(Integer, nullable=False)
    Hospital = relationship("Hospital")

    __table_args__ = (
        ForeignKeyConstraint(['MetricTypeId'], ['MetricType.MetricTypeId'],
                             name="fk__HospitalMetricCat__MetricType__MetricTypeId"),
        ForeignKeyConstraint(['HospitalId'], [
                             'Hospital.HospitalId'], name="fk__HospitalMetricCat__Hospital__HospitalId"),
        UniqueConstraint('HospitalId', 'MetricTypeId',
                         name="uc__HospitalMetricCat__HospitalId__MetricTypeId"),
    )

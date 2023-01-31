from datetime import datetime
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Float, String, DateTime
from sqlalchemy.sql.sqltypes import SMALLINT
from sqlalchemy.sql.schema import ForeignKeyConstraint

from modules.utilities.aws.rds.initialize.table_base import sqla_base


class HealthSystem(sqla_base):
    __tablename__ = "HealthSystem"
    HealthSystemId = Column(Integer, autoincrement=True, primary_key=True)
    Hsi = Column(String(11))
    HealthSystemName = Column(String(200))
    City = Column(String(50))
    AhaSystemId = Column(Integer)
    InOneKey = Column(SMALLINT)
    OneKeyId = Column(Integer)
    Latitude = Column(Float)
    Longitude = Column(Float)

    StateTypeId = Column(Integer, nullable=False)
    StateType = relationship("StateType")

    OwnershipTypeId = Column(Integer, nullable=True)
    OwnershipType = relationship("OwnershipType")

    HealthSystemContinuousMetricAssignments = relationship(
        "HealthSystemContinuousMetricAssignment", back_populates="HealthSystem")
    HealthSystemCategoricalMetricAssignments = relationship(
        "HealthSystemCategoricalMetricAssignment", back_populates="HealthSystem")
    HospitalMetricSystemMedians = relationship(
        "HospitalMetricSystemMedian", back_populates="HealthSystem")

    __table_args__ = (
        ForeignKeyConstraint(['StateTypeId'], [
                             'StateType.StateTypeId'], name="fk__HealthSystem__StateType__StateTypeId"),
        ForeignKeyConstraint(['OwnershipTypeId'], ['OwnershipType.OwnershipTypeId'],
                             name="fk__HealthSystem__OwnershipType__OwnershipTypeId"),
    )

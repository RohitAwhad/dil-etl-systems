from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import UniqueConstraint

from sqlalchemy import Column, Integer, String
from modules.utilities.aws.rds.initialize.table_base import sqla_base


class StateType(sqla_base):
    __tablename__ = "StateType"
    StateTypeId = Column(Integer, autoincrement=True, primary_key=True)
    StateTypeName = Column(String(50), unique=True)
    StateAbbreviation = Column(String(2), unique=True)

    CountyTypes = relationship("CountyType", back_populates="StateType")
    HealthSystems = relationship("HealthSystem", back_populates="StateType")
    StateTypeContinuousMetricAssignments = relationship(
        "StateTypeContinuousMetricAssignment", back_populates="StateType")

    __table_args__ = (
        UniqueConstraint('StateTypeName', 'StateAbbreviation',
                         name="uc__StateType__StateTypeName__StateAbbreviation"),
    )

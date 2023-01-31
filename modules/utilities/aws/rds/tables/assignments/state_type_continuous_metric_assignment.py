from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Float
from sqlalchemy.sql.schema import ForeignKeyConstraint, UniqueConstraint

from modules.utilities.aws.rds.initialize.table_base import sqla_base


class StateTypeContinuousMetricAssignment(sqla_base):
    __tablename__ = "StateTypeContinuousMetricAssignment"
    StateTypeContinuousMetricAssignmentId = Column(
        Integer, autoincrement=True, primary_key=True)
    Value = Column(Float, nullable=False)

    MetricTypeId = Column(Integer, nullable=False)
    MetricType = relationship("MetricType")

    StateTypeId = Column(Integer, nullable=False)
    StateType = relationship("StateType")

    __table_args__ = (
        ForeignKeyConstraint(['MetricTypeId'], [
                             'MetricType.MetricTypeId'], name="fk__StateMetricCont__MetricType__MetricTypeId"),
        ForeignKeyConstraint(['StateTypeId'], [
                             'StateType.StateTypeId'], name="fk__StateMetricCont__StateType__StateTypeId"),
        UniqueConstraint('StateTypeId', 'MetricTypeId',
                         name="uc__StateMetricCont__StateTypeId__MetricTypeId"),
    )

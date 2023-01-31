from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKeyConstraint, UniqueConstraint

from modules.utilities.aws.rds.initialize.table_base import sqla_base


class CountyType(sqla_base):
    __tablename__ = "CountyType"
    CountyTypeId = Column(Integer, autoincrement=True, primary_key=True)
    CountyTypeName = Column(String(50))

    StateTypeId = Column(Integer)
    StateType = relationship("StateType")

    Hospitals = relationship("Hospital", back_populates="CountyType")

    __table_args__ = (
        ForeignKeyConstraint(['StateTypeId'], [
                             'StateType.StateTypeId'], name="fk__CountyType__StateType__StateTypeId"),
        UniqueConstraint('CountyTypeName', 'StateTypeId',
                         name="uc__CountyType__CountyTypeName__StateTypeId"),
    )

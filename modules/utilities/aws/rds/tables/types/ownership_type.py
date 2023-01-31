from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import UniqueConstraint, ForeignKeyConstraint

from modules.utilities.aws.rds.initialize.table_base import sqla_base


class OwnershipType(sqla_base):
    __tablename__ = "OwnershipType"
    OwnershipTypeId = Column(Integer, autoincrement=True, primary_key=True)
    OwnershipTypeName = Column(String(30))

    HealthSystems = relationship(
        "HealthSystem", back_populates="OwnershipType")

    __table_args__ = (
        UniqueConstraint('OwnershipTypeName',
                         name="uc__OwnershipType__OwnershipTypeName"),
    )

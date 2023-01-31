from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.sql.sqltypes import SMALLINT
from sqlalchemy.sql.schema import ForeignKeyConstraint, UniqueConstraint


from modules.utilities.aws.rds.initialize.table_base import sqla_base


class Hospital(sqla_base):
    __tablename__ = "Hospital"
    HospitalId = Column(Integer, autoincrement=True, primary_key=True)
    CompendiumId = Column(String(20), nullable=True)
    CcnId = Column(String(7), nullable=True)
    HospitalName = Column(String(200))
    Address = Column(String(50))
    FullAddress = Column(String(200))
    City = Column(String(50))
    ZipCode = Column(Integer)
    Longitude = Column(Float)
    Latitude = Column(Float)
    IsCah = Column(SMALLINT)

    HealthSystemId = Column(Integer, nullable=True)
    HealthSystem = relationship("HealthSystem")

    CountyTypeId = Column(Integer, nullable=True)
    CountyType = relationship("CountyType")

    HospitalContinuousMetricAssignments = relationship(
        "HospitalContinuousMetricAssignment", back_populates="Hospital")
    HospitalCategoricalMetricAssignments = relationship(
        "HospitalCategoricalMetricAssignment", back_populates="Hospital")

    __table_args__ = (
        ForeignKeyConstraint(['CountyTypeId'], [
                             'CountyType.CountyTypeId'], name="fk__Hospital__CountyType__CountyTypeId"),
        ForeignKeyConstraint(['HealthSystemId'], [
                             'HealthSystem.HealthSystemId'], name="fk__Hospital__HealthSystem__HealthSystemId"),
        UniqueConstraint('CompendiumId', 'CcnId',
                         name="uc__Hospital__CompendiumId__CcnId"),
    )

# Example schema for part of the SIMPLE database

from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String, BigInteger, Enum, Date, DateTime
import enum
from astrodbkit2.astrodb import Base


# -------------------------------------------------------------------------------------------------------------------
# Reference tables
class Publications(Base):
    """ORM for publications table.
    This stores reference information (DOI, bibcodes, etc) and has shortname as the primary key
    """
    __tablename__ = 'Publications'
    name = Column(String(30), primary_key=True, nullable=False)
    bibcode = Column(String(100))
    doi = Column(String(100))
    description = Column(String(1000))


class Telescopes(Base):
    __tablename__ = 'Telescopes'
    name = Column(String(30), primary_key=True, nullable=False)
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'))


class Instruments(Base):
    __tablename__ = 'Instruments'
    name = Column(String(30), primary_key=True, nullable=False)
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'))


# -------------------------------------------------------------------------------------------------------------------
# Enumerations tables
class Regime(enum.Enum):
    """Enumeration for spectral type regime"""
    optical = 'optical'
    infrared = 'infrared'
    ultraviolet = 'ultraviolet'
    radio = 'radio'


# -------------------------------------------------------------------------------------------------------------------
# Main tables
class Sources(Base):
    """ORM for the sources table. This stores the main identifiers for our objects along with ra and dec"""
    __tablename__ = 'Sources'
    source = Column(String(100), primary_key=True, nullable=False)
    ra = Column(Float)
    dec = Column(Float)
    shortname = Column(String(30))  # not needed?
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'), nullable=False)
    comments = Column(String(1000))


class Names(Base):
    __tablename__ = 'Names'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade'), nullable=False, primary_key=True)
    other_name = Column(String(100), primary_key=True, nullable=False)


class Photometry(Base):
    __tablename__ = 'Photometry'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'), nullable=False, primary_key=True)
    band = Column(String(30), primary_key=True)
    ucd = Column(String(100))
    magnitude = Column(Float)
    magnitude_error = Column(Float)
    telescope = Column(String(30), ForeignKey('Telescopes.name', ondelete='cascade'))
    instrument = Column(String(30), ForeignKey('Instruments.name', ondelete='cascade'))
    epoch = Column(String(30))
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'), primary_key=True)


class SpectralTypes(Base):
    __tablename__ = 'SpectralTypes'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'), nullable=False, primary_key=True)
    spectral_type = Column(Float)
    spectral_type_error = Column(Float)
    regime = Column(Enum(Regime, create_constraint=True), primary_key=True)  # restricts to a few values: Optical, Infrared
    best = Column(Boolean)  # flag for indicating if this is the best measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'), primary_key=True)

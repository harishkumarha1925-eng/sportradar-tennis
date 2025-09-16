# models.py
from sqlalchemy import (
    Column, Integer, String, ForeignKey, Text
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Category(Base):
    __tablename__ = "categories"
    category_id = Column(String(50), primary_key=True)
    category_name = Column(String(100), nullable=False)
    competitions = relationship("Competition", back_populates="category")

class Competition(Base):
    __tablename__ = "competitions"
    competition_id = Column(String(50), primary_key=True)
    competition_name = Column(String(200), nullable=False)
    parent_id = Column(String(50), nullable=True)
    type = Column(String(50), nullable=False)
    gender = Column(String(20), nullable=False)
    category_id = Column(String(50), ForeignKey("categories.category_id"))
    category = relationship("Category", back_populates="competitions")

class Complex(Base):
    __tablename__ = "complexes"
    complex_id = Column(String(50), primary_key=True)
    complex_name = Column(String(100), nullable=False)
    venues = relationship("Venue", back_populates="complex")

class Venue(Base):
    __tablename__ = "venues"
    venue_id = Column(String(50), primary_key=True)
    venue_name = Column(String(100), nullable=False)
    city_name = Column(String(100), nullable=False)
    country_name = Column(String(100), nullable=False)
    country_code = Column(String(3), nullable=False)
    timezone = Column(String(100), nullable=False)
    complex_id = Column(String(50), ForeignKey("complexes.complex_id"))
    complex = relationship("Complex", back_populates="venues")

class Competitor(Base):
    __tablename__ = "competitors"
    competitor_id = Column(String(50), primary_key=True)
    name = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)
    country_code = Column(String(3), nullable=False)
    abbreviation = Column(String(10), nullable=True)
    rankings = relationship("CompetitorRanking", back_populates="competitor")

class CompetitorRanking(Base):
    __tablename__ = "competitor_rankings"
    rank_id = Column(Integer, primary_key=True, autoincrement=True)
    rank = Column(Integer, nullable=False)
    movement = Column(Integer, nullable=False)
    points = Column(Integer, nullable=False)
    competitions_played = Column(Integer, nullable=False)
    competitor_id = Column(String(50), ForeignKey("competitors.competitor_id"))
    competitor = relationship("Competitor", back_populates="rankings")

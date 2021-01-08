from sqlalchemy import Column, Date, Float, Integer, String

from database import Base


class Offer(Base):
    __tablename__ = "offers"

    id = Column(Integer, primary_key=True, index=True)
    lon = Column(Float)
    lat = Column(Float)
    url = Column(String)
    added = Column(Date)
    title = Column(String)
    size = Column(Float)
    price = Column(Float)
    price_m2 = Column(Float)
    estimate = Column(Float)
    offer_type = Column(String)

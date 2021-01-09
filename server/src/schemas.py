import datetime
from typing import List, Optional

from pydantic import BaseModel


class OfferBase(BaseModel):
    lon: float
    lat: float
    url: str
    added: datetime.date
    title: str
    size: float
    price: float
    price_m2: float
    estimate: float
    offer_type: str
    offer_id: str
    price_estimate_diff: float


class OfferCreate(OfferBase):
    pass

class Offer(OfferBase):
    id: int
    class Config:
        orm_mode = True

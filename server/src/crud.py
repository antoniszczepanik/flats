import datetime
from sqlalchemy.orm import Session

import models
import schemas


def get_offer(db: Session, offer_id: int):
    return db.query(models.Offer).filter(models.Offer.id == offer_id).first()

def get_offers(
    db: Session,
    from_date: datetime.date = None,
    to_date: datetime.date = None,
    offer_type: str = None,
    max_price: float = None,
    min_price: float = None,
    max_size: float = None,
    min_size: float = None,
    min_price_estimate_diff: float = None,
):
    offers = db.query(models.Offer)
    if offer_type:
       offers = offers.filter(models.Offer.offer_type == offer_type)
    if from_date:
       offers = offers.filter(models.Offer.added >= from_date)
    if to_date:
       offers = offers.filter(models.Offer.added <= to_date)
    if max_price:
       offers = offers.filter(models.Offer.price <= max_price)
    if min_price:
       offers = offers.filter(models.Offer.price >= min_price)
    if max_size:
       offers = offers.filter(models.Offer.size <= max_size)
    if min_size:
       offers = offers.filter(models.Offer.size >= min_size)
    if min_price_estimate_diff:
       offers = offers.filter(models.Offer.price_estimate_diff >= min_price_estimate_diff)
    return offers.all()


def create_offer(db: Session, offer: schemas.OfferCreate):
    db_offer = models.Offer(
        **offer.dict()
    )
    db.add(db_offer)
    db.commit()
    db.refresh(db_offer)
    return db_offer

from sqlalchemy.orm import Session

import models
import schemas


def get_offer(db: Session, offer_id: int):
    return db.query(models.Offer).filter(models.Offer.id == offer_id).first()

def get_offers(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Offer).offset(skip).limit(limit).all()


def create_offer(db: Session, offer: schemas.OfferCreate):
    db_offer = models.Offer(
        # **offer.dict() could also be used instead
        lon=offer.lon,
        lat=offer.lat,
        url=offer.url,
        added=offer.added,
        title=offer.title,
        size=offer.size,
        price=offer.price,
        price_m2=offer.price_m2,
        estimate=offer.estimate,
        offer_type=offer.offer_type,
    )
    db.add(db_offer)
    db.commit()
    db.refresh(db_offer)
    return db_offer

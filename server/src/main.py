from typing import List

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

import crud
import models
import schemas
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/offers/", response_model=schemas.Offer)
def create_offer(offer: schemas.OfferCreate, db: Session = Depends(get_db)):
    return crud.create_offer(db=db, offer=offer)


@app.get("/offers/", response_model=List[schemas.Offer])
def read_offers(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    offers = crud.get_offers(db, skip=skip, limit=limit)
    return offers


@app.get("/offers/{offer_id}", response_model=schemas.Offer)
def read_offer(offer_id: int, db: Session = Depends(get_db)):
    db_offer = crud.get_offer(db, offer_id=offer_id)
    if db_offer is None:
        raise HTTPException(status_code=404, detail="Offer not found")
    return db_offer

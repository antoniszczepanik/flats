import datetime
from typing import List

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import pandas as pd

from add_features import get_coords_factor
import crud
import columns as c
import models
import schemas
from database import SessionLocal, engine
from fs_client import FsClient

models.Base.metadata.create_all(bind=engine)
fs = FsClient()
model = fs.read_newest_model("flats-models/{data_type}/models", dtype="sale")
app = FastAPI()

origins = [
    "http://127.0.0.1:5500",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/api/v1/offers/", response_model=schemas.Offer)
def create_offer(offer: schemas.OfferCreate, db: Session = Depends(get_db)):
    return crud.create_offer(db=db, offer=offer)


@app.get("/api/v1/offers/", response_model=List[schemas.Offer])
def read_offers(
    from_date: datetime.date = None,
    to_date: datetime.date = None,
    offer_type: str = None,
    max_price: float = None,
    min_price: float = None,
    max_size: float = None,
    min_size: float = None,
    min_price_estimate_diff: float = None,
    db: Session = Depends(get_db),
):
    offers = crud.get_offers(
        db,
        from_date=from_date,
        to_date=to_date,
        offer_type=offer_type,
        max_price=max_price,
        min_price=min_price,
        max_size=max_size,
        min_size=min_size,
        min_price_estimate_diff=min_price_estimate_diff,
    )
    return offers


@app.get("/api/v1/offers/{offer_id}", response_model=schemas.Offer)
def read_offer(offer_id: int, db: Session = Depends(get_db)):
    db_offer = crud.get_offer(db, offer_id=offer_id)
    if db_offer is None:
        raise HTTPException(status_code=404, detail="Offer not found")
    return db_offer

@app.get("/api/v1/predict/", response_model=schemas.PricePrediction)
def get_prediction(
    size: float,
    floor: int,
    floor_n: int,
    building_year: int,
    lon: float,
    lat: float,
):
    coords_factor = get_coords_factor(lon, lat)
    return {"prediction": size * model.predict([[
        coords_factor,
        floor_n,
        size,
        floor,
        building_year,
        0, # view count placeholder
        0, # desc len placeholder
        floor_n,
        lon,
        lat,
    ]])}

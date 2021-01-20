# [Flats](http://flats.antoniszczepanik.com)

1. Rent & sale offers are scraped and valuated.
2. Offer price is compared with valuations.
3. You can filter & preview most interesting ones on map.

[picture]

# Technical description

The project is structured as 3 components.

- Scraper which scrapes & valuates the offers.
- Server (REST + Postgres) allowing to query the data.
- Client, just to present the results on a map.

## Scraper

Scraper works as follows:
1. It scrapes offers.
2. Data is cleaned, outliers are filtered, new features are added, etc
3. Newest of previously trained models is applied.
4. Offers are uploaded through API.

Additionally one can run one of on demand tasks
- Model training, which will read raw scraped data after specified date.
- "Coords map" generation. This is a table of neigborhoods with mean prices & additional features assigned. 
  It will be stored in MinIO and used when applying the model.

### Model
The model is Random Forest Regressor.
It's trained on all historically scraped data, so the valuation is of offer price.
Currently the error is around 14% nRMSE for both sale and rent offers.

## Webserver
REST API with Postgres backend, written in FastAPI, Pyhon.
Here is chart of the architecture.

## Client
The client has been written in vanilla Javascript and Leaflet was used for mapping capabilities.


# How to run it?

All components in this project are packaged as docker containers.
In order to run it you have to have `docker` and `docker-comspose` installed.

First run server and database containers, so to allow data to be stored.

`cd server && docker-compose up -d && cd ..`

After that you can scrape first offers. MinIO is used as local S3 storage so you have to set it up first:

`cd scraper && ./setup_minio.sh`

If you'd like to mount MinIO elsewhere than `~/flats_buckets` take a look at `./setup_minio.sh` script.
Then we can run container that scrapes and processes the data.

TODO: Generate coord map and 

`./run.sh scrape sale && ./run.sh process sale`

And you just scraped and valuated first batch of offers.

If you'd like to learn more about the script you can

`./run.sh --help`



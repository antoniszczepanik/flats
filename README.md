# [Flats](http://flats.antoniszczepanik.com)
Detect undervalued real estate offers by finding differences between valuation and offer prices.

Set of ETL and ML pipelines designed to valuate flat sale and rent offers.
Estimates are assigned to each scraped item and compared to actual prices,
which allows to instantly identify real estate opportunities.
The pipeline is run daily and results are presented on [queryable website](http://flats.antoniszczepanik.com).

[![alt text](docs/iletomieszkanie15082020.png)](http://flats.antoniszczepanik.com)
=======
# [IleToMieszkanie](http://flats.antoniszczepanik.com)

1. Rent & sale offers are scraped and valuated.
2. Offer price is compared with valuations.
3. You can filter most interesing & preview most interesting ones on map.

[picture]

# How to run the project?

All components in this project are packaged as docker containers.
In order to run it you have to have `docker` and `docker-comspose` installed.

First run server and database containers, so to allow data to be stored.

`cd server && docker-compose up -d && cd ..`

After that you can scrape first offers. MinIO is used as local S3 storage so you have to set it up first:

`cd scraper && ./setup_minio.sh`

If you'd like to mount MinIO elsewhere take a look at `./setup_minio.sh` script.
Then we can run container that scrapes and processes the data.

`./run.sh scrape sale && ./run.sh process sale`


# Technical description

The project is structured as 3 components.

- Scraper, which scrapes & valuates the offer. (model is applied as part of scraping pipeline)
- Server with Postgres in the background, allowing to query large amount of data.
- Client, just to present the results in digestable form.

## Scraper
- scraper process architecture
- model

## Webserver
- technologies
- [png]

## Client
- Vanilla Javascript + Leaflet

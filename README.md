# [Flats](http://flats.antoniszczepanik.com)
Detect undervalued real estate offers by finding differences between valuation and offer prices.

Set of ETL and ML pipelines designed to valuate flat sale and rent offers.
Estimates are assigned to each scraped item and compared to actual prices,
which allows to instantly identify real estate opportunities.
The pipeline is run daily and results are presented on [queryable website](http://flats.antoniszczepanik.com).

[![alt text](docs/iletomieszkanie15082020.png)](http://flats.antoniszczepanik.com)
=======
# [IleToMieszkanie](http://flats.antoniszczepanik.com)

Detect undervalued real estate offers by finding differences between valuation and offer prices.

# How to run the project? (manual)

All components in this project are packaged as docker containers.
In order to run it you have to have `docker` and `docker-comspose` installed.


# Technical description

The architecture of the project is structured as 3 components

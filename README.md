# [Flats](flats.antoniszczepanik.com)
Detect undervalued real estate offers by finding differences between valuation and offer prices.

Set of ETL and ML pipelines designed to valuate flat sale and rent offers.
Estimates are assigned to each scraped item and compared to actual prices,
which allows to instantly identify real estate opportunities.
The pipeline is run daily and results are presented on [queryable website](flats.anotniszczepanik.com).

[website screenshot]

### Why?

Self interest
Ability to play with airflow
Creating really useful tool


### Pipeline architecture
Airflow single dag (screenshot). Separated for rent and sale in parallel.
Pipeline overview. Two tasks are offline to capitalize on compute and allows for CI.
Describe process of adding additional geo-location factors (clustering etc0
Deployed on AWS and presented as S3 static (almost for free)
[Screenshot of airflow gui, pipeline flow chart]

### Development
Proud  - website can be updated in few seconds, pipeline is deployed via githook.
Docker containers which can be run either locally, or on prod with reverse-proxy nginx.


### Valuation quality
Data sizes, error and which models with which HP were selected. Most significant features.
[Cluster screenshots, model error visualisation]

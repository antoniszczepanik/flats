# [Flats](http://www.flats.antoniszczepanik.com)
Detect undervalued real estate offers by finding differences between valuation and offer prices.

Set of ETL and ML pipelines designed to valuate flat sale and rent offers.
Estimates are assigned to each scraped item and compared to actual prices,
which allows to instantly identify real estate opportunities.
The pipeline is run daily and results are presented on [queryable website](http://flats.antoniszczepanik.com).

[![alt text](docs/WebsiteScreenshot.png)](http://flats.antoniszczepanik.com)

### Why?

Idea grown purely out of my self-interest. Being tired of scrolling offer sites
looking for a flat to rent I decided to automate the process as much as possible.
Additionally I had some experience developing real estate valuation models so I
naturally connected both of these things.


Also, I find creating useful, pragmatic tools to be the most rewarding.

### TODO:

### Quality of predictions

One of the most common questions is how accurate valuation model actually is.
The metric used to access models quality is Median Absolute Percentage Error,
which allows for [...]
The results achieved by both models

Which model is used? Which HP were selected?

Most significant features?

[Cluster screenshots, model error visualisation]


### Pipeline architecture
Airflow single dag (screenshot). Separated for rent and sale in parallel.
Pipeline overview. Two tasks are offline to capitalize on compute and allows for CI.
Describe process of adding additional geo-location factors (clustering etc0
Deployed on AWS and presented as S3 static (almost for free)
[Screenshot of airflow gui, pipeline flow chart]


### Development
Proud  - website can be updated in few seconds, pipeline is deployed via githook.
Docker containers which can be run either locally, or on prod with reverse-proxy nginx.




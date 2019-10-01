### Currently the project consists of 2 elements:
- webcrawler which scrapes Polish flats rent and sale offers daily.
- data cleaning and feature engineering pipelines.

The goal of this project is producing possibly best real estate evaluation model and serving it as an API.

As of October 2019 modelling is based on around 100k rent offers and 150k sale offers.

Webcrawler and all pipelines are deployed on an EC2 instance. Data is stored on S3 at each stage of the processing.

![General Overview](docs/FlatsOverview.png)


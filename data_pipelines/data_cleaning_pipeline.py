"""
Pipeline to clean concatinated morizon data.
It saves cleaned data on s3 in 'morizon-data/morizon_sale_cleaned' folder.
It drops only text based columns ('desc', 'img_link', ...)
The rest is converted to numerical based on dictionaries
defined below.

TODO:
- create folders on s3 with names: morizon_sale_concated, morizon_rent_concated
morizon_sale_cleaned, morizon_rent_cleaned
- change somewhere in scrapy settings to save in filenames: morizon_sale_scraped,
morizon_rent_scraped
- change catching all exceptions to catching specified exceptions or remove them
in s3_bucket.py
- verify if data_concatinatioin  works and logs properly
- add already present sale_files to morizon_sale and rent folder to concat them
- backup first concated file to google drive
- figure out how to view logs of current processes in webservice - s3 static?
minimal view only to be able to read logs from whereever
- when to run pipelines? how often? figure out easy way to run them manually
when needed, else run them maybe weekly?
- run a pipeline to scrape all images from links in concatinated files - untill
its possible!

""""

#datetime = datetime.now().strptime()
#log.basicConfig(filename='cleaning{datetime}.log', format='%(name)s - %(levelname)s - %(message)s')

 FROM python:3
  
 # Set the working directory to /usr/src/app.
 WORKDIR /usr/src/app
  
 # Copy the file from the local host to the filesystem of the container at the working directory.
 COPY requirements.txt ./
  
 # Install packages from requirements.txt.
 RUN pip3 install --no-cache-dir -r requirements.txt
  
 # Copy the project source code from the local host to the filesystem of the container at the working directory.
 COPY . .
  
 # Run the crawler when the container launches.
 CMD [ "scrapy", "crawl", "morizon", "-o", "morizon_trail.csv"]

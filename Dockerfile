 FROM python:3
  
 # Set the working directory to /usr/src/app.
 WORKDIR /usr/src/app

 # Get gdrive for linux
 RUN mkdir ../gdrive &&
     wget -P ../gdrive 'https://drive.google.com/uc?id=1Ej8VgsW5RgK66Btb9p74tSdHMH3p4UNb&export=download' &&
     chmod +x gdrive
  
 # Copy the file from the local host to the filesystem of the container at the working directory.
 COPY requirements.txt ./
 COPY .gdrive ~/ 
 # Install packages from requirements.txt.
 RUN pip3 install --no-cache-dir -r requirements.txt
  
 # Copy the project source code from the local host to the filesystem of the container at the working directory.
 COPY . .
  
 # Run the crawler when the container launches.
 CMD [ "scrapy", "crawl", "morizon_sale", "-o", "morizon_trail.csv"]

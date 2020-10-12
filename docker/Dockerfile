FROM python:3.7-slim-stretch
LABEL maintainer="szczepanik.antoni@gmail.com"

COPY requirements.txt /requirements.txt

RUN apt-get update && apt-get install -y gcc libc-dev libffi-dev python3-dev libxml2-dev libxslt-dev 
RUN pip install -r /requirements.txt

# copy dags and deps
COPY . /usr/local/code

# add deps to path
ENV PYTHONPATH=$PYTHONPATH:/usr/local/code
ENV PYTHONPATH=$PYTHONPATH:/usr/local/code/spider

WORKDIR /usr/local/code
ENTRYPOINT ["/usr/local/bin/python3", "entrypoint.py"]

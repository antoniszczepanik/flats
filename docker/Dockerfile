FROM python:3.7-buster
LABEL maintainer="szczepanik.antoni@gmail.com"

RUN apt-get update && apt-get install -y gcc
COPY requirements.txt /requirements.txt
# use piwheels to download packages on pi faster
COPY pip.conf /etc/pip.conf

RUN pip install -r /requirements.txt

# copy dags and deps
COPY . /usr/local/code

# add deps to path
ENV PYTHONPATH=$PYTHONPATH:/usr/local/code
ENV PYTHONPATH=$PYTHONPATH:/usr/local/code/spider

WORKDIR /usr/local/code
ENTRYPOINT ["/usr/local/bin/python3", "entrypoint.py"]

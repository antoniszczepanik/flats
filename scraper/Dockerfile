FROM python:3.7-buster
LABEL maintainer="szczepanik.antoni@gmail.com"

RUN apt-get update && apt-get install -y libatlas-base-dev libgeos-c1v5 libgeos-3.7.1

COPY src/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# copy dags and deps
COPY /src /usr/local/src

# add deps to path
ENV PYTHONPATH=$PYTHONPATH:/usr/local/code
ENV PYTHONPATH=$PYTHONPATH:/usr/local/code/spider

WORKDIR /usr/local/src
ENTRYPOINT ["/usr/local/bin/python3", "entrypoint.py"]

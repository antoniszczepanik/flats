FROM python:3.7-buster
LABEL maintainer="szczepanik.antoni@gmail.com"

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

# copy dags and deps
COPY . /usr/local/code

# add deps to path
ENV PYTHONPATH=$PYTHONPATH:/usr/local/code
ENV PYTHONPATH=$PYTHONPATH:/usr/local/code/spider

WORKDIR /usr/local/code
ENTRYPOINT ["/usr/local/bin/python3", "entrypoint.py"]

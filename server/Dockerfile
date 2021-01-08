FROM python:3.9

COPY requirements.txt /requirements.txt
RUN pip install -r requirements.txt


COPY src /src
WORKDIR /src

CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--reload"]

FROM ubuntu:18.04

RUN apt-get update && \
    apt-get -y install build-essential \
                       python3-dev \
		       python3-pip \
		       s3fs

COPY requirements.txt /home/requirements.txt
RUN pip3 install -r /home/requirements.txt

#ENV PATH="~/.local/bin:${PATH}"
#ENV PYTHONPATH="/home/flats:${PYTHONPATH}"

EXPOSE 80




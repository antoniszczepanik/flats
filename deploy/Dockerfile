FROM ubuntu:18.04
MAINTAINER Antoni Szczepanik

# Do not prompt while installing
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

RUN set -ex \
    && buildDeps=' \
        python3-dev \
        python3-pip \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        netcat \
        libspatialindex-dev \
        locales \
        netcdf-bin \
        curl \
        sudo \
    && python3 -m pip install -U pip setuptools wheel \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY ./requirements.txt /requirements.txt
RUN pip3 install -r requirements.txt

WORKDIR "/user/local"
RUN git clone https://github.com/kabuboy/flats.git
WORKDIR "/user/local/flats"

COPY ./deploy.sh /deploy.sh
#TODO: Figure out how to run a setup script
RUN /bin/bash /deploy.sh

EXPOSE 6800

ENTRYPOINT ["/bin/bash"]

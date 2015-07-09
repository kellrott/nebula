#FROM python:2.7.9-slim

FROM ubuntu

RUN apt-get update && apt-get install -y curl g++ lib32z1-dev \
make libapr1-dev libsvn-dev libcurl4-nss-dev libsasl2-dev python-dev python-pip

RUN pip install requests

WORKDIR /opt
RUN curl -O http://mirror.metrocast.net/apache/mesos/0.22.1/mesos-0.22.1.tar.gz

RUN tar xvzf mesos-0.22.1.tar.gz && cd mesos-0.22.1 \
&& ./configure --prefix /opt/mesos --disable-java \
&& make && make install && easy_install src/python/dist/*.egg && cd /opt && rm -rf /opt/mesos-0.22.1

#RUN pip install pesos

RUN  mkdir /opt/python
ENV  PYTHONPATH  /opt/python
ADD nebula /opt/python/nebula
ADD galaxy /opt/python/galaxy
ADD bin /opt/bin
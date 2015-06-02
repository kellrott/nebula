FROM python:2.7.9-slim

RUN  mkdir /opt/python
ENV  PYTHONPATH  /opt/python
ADD nebula /opt/python/nebula
ADD galaxy /opt/python/galaxy

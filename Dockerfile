FROM bgruening/galaxy-stable:lite

COPY ./ /opt/nebula

RUN cd /opt/nebula && python setup.py build && python setup.py install
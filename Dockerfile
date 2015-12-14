FROM bgruening/galaxy-stable:dev

COPY ./ /opt/nebula

RUN cd /opt/nebula && python setup.py build && python setup.py install

FROM bgruening/galaxy-stable:dev

#GET GRPC installed
RUN echo "deb http://http.debian.net/debian jessie-backports main" >> /etc/apt/sources.list
RUN apt-get update && apt-get install -y --force-yes libgrpc-dev

RUN cd /opt && git clone https://github.com/kellrott/agro.git
RUN cd /opt/agro && python setup.py build && python setup.py install

RUN cd /opt && git clone https://github.com/kellrott/nebula.git && cd nebula && git checkout dev  && python setup.py build && python setup.py install
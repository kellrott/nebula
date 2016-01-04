FROM golang:1.5.2-wheezy

WORKDIR /opt
RUN git clone https://github.com/kellrott/agro.git

ENV GOPATH /opt/agro
RUN cd /opt/agro && go get -d agro-manager && go get -d agro-worker
RUN cd /opt/agro && go install agro-manager && go install agro-worker
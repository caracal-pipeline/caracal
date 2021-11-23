FROM ubuntu:20.04
MAINTAINER <sphemkha@gmail.com>
ENV USER root
RUN apt update
ENV MYPV 3.6.9
RUN apt install build-essential checkinstall zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget tar git -y
RUN wget https://www.python.org/ftp/python/${MYPV}/Python-${MYPV}.tgz
RUN tar xzvf Python-${MYPV}.tgz
WORKDIR Python-${MYPV}
RUN ./configure --enable-optimizations
RUN make altinstall
WORKDIR /
RUN apt install python3-pip -y 
#RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.6 10 
#RUN pip3 install --upgrade --force pip
RUN python3.6 --version
RUN ls /usr/bin/python*
RUN python3.6 -m pip install -U pip
RUN python3.6 --version
RUN python3.6 -m pip install montagepy
RUN pip install flake8 pytest
ADD . /build
#RUN pip install /build
#RUN caracal -h

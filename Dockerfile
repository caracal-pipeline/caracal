FROM stimela/base:1.0.1
MAINTAINER <sphemkha@gmail.com>
ADD . /build
ENV USER root
RUN pip install /build
RUN meerkathi -h

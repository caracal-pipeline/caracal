FROM stimela/base:0.3.1
MAINTAINER <sphemkha@gmail.com>
ADD . /build
ENV USER root
RUN pip install /build
RUN meerkathi --help

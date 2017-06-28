FROM stimela/base:0.2.9
MAINTAINER <sphemkha@gmail.com>
ADD . /build
ENV USER root
RUN pip install /build
RUN meerkathi --help

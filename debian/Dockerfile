FROM ubuntu:20.04
MAINTAINER Minh-Tri Pham

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install --yes \
        software-properties-common \
        build-essential \
        debhelper \
        devscripts \
        equivs \
        python3-dev

RUN add-apt-repository ppa:spotify-jyrki/dh-virtualenv

ADD ./debian/control /tmp/python3-mtbase/debian/control
RUN mk-build-deps --install /tmp/python3-mtbase/debian/control --tool "apt-get --allow-downgrades --yes"

WORKDIR /tmp/python3-mtbase

ADD . /tmp/python3-mtbase

FROM python:3.7-slim

WORKDIR /opt/app-root

RUN pip install --upgrade pip && \
    apt-get update -qq && \
    apt-get -y --no-install-recommends install curl wget

RUN pip install selenium==3.141.0 fasteners cachetools

COPY generic/generic_ateb_subscriber.py .

# use entrypoint, more idiomatic for passing different command line args
ENTRYPOINT [ "python", "generic_ateb_subscriber.py"]

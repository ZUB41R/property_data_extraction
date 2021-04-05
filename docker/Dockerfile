#  Author: Zubair Ahmed <zub41r.ahm3d@gmail.com>


FROM ubuntu:20.04

ARG _PY_VERSION=3
ARG _PIP_VERSION=20.0.2
ARG PYTHON=python${_PY_VERSION}
ARG PIP=pip${_PY_VERSION}


LABEL maintainer="zub41r.ahm3d@gmail.com"
RUN echo "[Container Setup] Building Property Scraper Service Development Container!"


# Python and PIP
#################
# Install python and pip and make sure they are uptodate.
RUN echo Installing $PYTHON and $PIP

# See http://bugs.python.org/issue19846
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

RUN apt-get update && apt-get install -y \
    ${PYTHON} \
    ${PYTHON}-pip

RUN ${PIP} --no-cache-dir install --upgrade \
    pipenv \
    setuptools==46.0.0

# Python Libraries
#################
COPY requirements.txt requirements.txt
RUN pip3 --no-cache-dir install -r requirements.txt

# Cleanup
RUN apt-get autoremove -y

# Application Setup
#################
# Create the working directory
RUN set -ex && mkdir /app

WORKDIR /app
EXPOSE 8888
ENV PYTHONPATH="$PYTHONPATH:/app"

COPY utils /app/utils
COPY scraper /app/scraper
COPY app.py /app/app.py

# COPY .git/ /worker/.git/
# COPY .gitignore /worker

# COPY add_user.sh /
# ENTRYPOINT ["/add_user.sh"]
CMD ["bash", "-c", "source /etc/bash.bashrc && python3 /app/app.py run"]
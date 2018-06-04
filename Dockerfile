FROM python:3.6
LABEL maintainer="tkukral@mirantis.com"

# prepare directory
WORKDIR /code

# install dependencies
RUN set -x && \
  echo "deb http://ppa.launchpad.net/ansible/ansible/ubuntu trusty main" >> /etc/apt/sources.list && \
  apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 93C4A3FD7BB9C367 && \
  apt-get update && \
  apt-get install --no-install-recommends -y libsasl2-dev python-dev libldap2-dev libssl-dev ansible ssh python-netaddr && \
  rm -rf /var/lib/apt/lists/* && \
  mkdir /var/log/kqueen-api

# copy app
COPY . .
RUN pip install .


# run app
CMD ./entrypoint.sh

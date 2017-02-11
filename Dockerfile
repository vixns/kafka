FROM openjdk:8

COPY . /src

RUN cd /src \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF \
    && curl -s https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add - \
    && apt-get update \
    && apt-get install --no-install-recommends -y apt-transport-https \
    && echo "deb http://repos.mesosphere.com/debian jessie-unstable main" | tee /etc/apt/sources.list.d/mesosphere.list \
    && echo "deb http://repos.mesosphere.com/debian jessie-testing main" | tee -a /etc/apt/sources.list.d/mesosphere.list \
    && echo "deb http://repos.mesosphere.com/debian jessie main" | tee -a /etc/apt/sources.list.d/mesosphere.list \
    && echo 'deb https://deb.nodesource.com/node_7.x jessie main' | tee /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y --force-yes \
    mesos=1.1.1\* \
    maven \
    nodejs \
    scala \
    libcurl3-nss \
    && ln -sf /usr/bin/nodejs /usr/bin/node \
	&& ./gradlew clean jar -x test \
	&& wget http://apache.mirrors.ovh.net/ftp.apache.org/dist/kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz \
	&& mkdir /opt/kafka-mesos && mv /src/kafka* /opt/kafka-mesos/ \
	&& mv run.sh /run.sh && chmod 555 /run.sh \
	&& rm -rf /src /root/.gradle 

WORKDIR /opt/kafka-mesos
EXPOSE 7000
ENTRYPOINT ["/run.sh"]

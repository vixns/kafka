FROM openjdk:8

COPY . /src

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF \
    && echo "deb http://repos.mesosphere.com/debian jessie main" | tee -a /etc/apt/sources.list.d/mesosphere.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y --force-yes \
    mesos=1.2.0\* \
    maven \
    scala \
    libcurl3-nss \
    && cd /src && ./gradlew clean jar -x test \
    && mv /src/kafka*jar / \
    && mv /src/run.sh /run.sh && chmod 555 /run.sh

RUN git clone https://github.com/vixns/dropwizard-prometheus.git \
    && cd dropwizard-prometheus \
    && mvn clean package \
    && mv target/dropwizard-prometheus-0.0.1-SNAPSHOT.jar / \
    && dpkg --purge maven scale && apt-get -y autoremove \
    && cd / \
    && wget -q https://archive.apache.org/dist/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz \
    && rm -rf /src /root/.gradle /var/lib/apt/*

WORKDIR /
EXPOSE 7000
ENTRYPOINT ["/run.sh"]

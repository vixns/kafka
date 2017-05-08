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
    && mv /src/run.sh /run.sh && chmod 555 /run.sh \
    && dpkg --purge maven scale && apt-get -y autoremove \
    && cd / && rm -rf /src /root/.gradle /var/lib/apt/*

EXPOSE 7000
ENTRYPOINT ["/run.sh"]

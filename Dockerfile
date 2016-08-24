FROM vixns/mesos

COPY . /src
RUN cd /src && ./gradlew clean jar -x test && \
wget http://apache.mirrors.ovh.net/ftp.apache.org/dist/kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz && \
mkdir /opt/kafka-mesos && mv /src/kafka* /opt/kafka-mesos/ && \
mv run.sh /run.sh && chmod 555 /run.sh && \
rm -rf /src /root/.gradle 

WORKDIR /opt/kafka-mesos
EXPOSE 7000
ENTRYPOINT ["/run.sh"]

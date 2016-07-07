FROM vixns/mesos

COPY . /src
RUN cd /src && ./gradlew jar && \
wget http://apache.trisect.eu/kafka/0.9.0.1/kafka_2.11-0.9.0.1.tgz && \
mkdir /opt/kafka-mesos && mv /src/kafka* /opt/kafka-mesos/ && \
rm -rf /src /root/.gradle

WORKDIR /opt/kafka-mesos
EXPOSE 7000
ENTRYPOINT ["java", "-jar", "/opt/kafka-mesos/kafka-mesos-0.9.5.1.jar", "scheduler"]

#!/bin/sh

NODE_ADDRESS=$(ip addr | awk '/inet/ && /eth/{sub(/\/.*$/,"",$2); print $2}')

exec java ${JAVA_OPTS} -jar /opt/kafka-mesos/kafka-mesos-0.9.5.1.jar scheduler --api=http://${NODE_ADDRESS}:7000 $@

#!/bin/sh

NODE_ADDRESS=$(ip addr | awk '/inet/ && /eth/{sub(/\/.*$/,"",$2); print $2}')
export MESOS_NATIVE_LIBRARY=/usr/lib/libmesos.so
export MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so
exec java ${JAVA_OPTS} -jar /kafka-mesos-*.jar scheduler --api=http://${NODE_ADDRESS}:7000 $@

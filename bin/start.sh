#!/bin/bash

#DEBUG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=8000,suspend=n"

WHEREAMI=`dirname "${0}"`
if [ -z "${MARGINLOADER_ROOT}" ]; then
    export MARGINLOADER_ROOT=`cd "${WHEREAMI}/../" && pwd`
fi

export LOG_LEVEL="${LOG_LEVEL:-info}"

MARGINLOADER_LIB=${MARGINLOADER_ROOT}/lib
MARGINLOADER_ETC=${MARGINLOADER_ROOT}/etc

java ${JAVA_OPTS} ${DEBUG} \
     -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
     -Dlogback.configurationFile=${MARGINLOADER_ETC}/logback.xml \
     -Ddave.configurationFile=${MARGINLOADER_ETC}/marginloader.conf \
     -jar ${MARGINLOADER_LIB}/dave-margin-loader-1.0-SNAPSHOT-fat.jar

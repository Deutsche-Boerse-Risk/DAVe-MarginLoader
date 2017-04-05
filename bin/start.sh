#!/bin/bash

#DEBUG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=8000,suspend=n"

WHEREAMI=`dirname "${0}"`
if [ -z "${MARGINLOADER_ROOT}" ]; then
    export MARGINLOADER_ROOT=`cd "${WHEREAMI}/../" && pwd`
fi

MARGINLOADER_LIB=${MARGINLOADER_ROOT}/lib
MARGINLOADER_ETC=${MARGINLOADER_ROOT}/etc
export MARGINLOADER_LOG=${MARGINLOADER_ROOT}/log

java ${DEBUG} \
     -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
     -Dlogback.configurationFile=${MARGINLOADER_ETC}/logback.xml \
     -Ddave.configurationFile=${MARGINLOADER_ETC}/marginloader.conf \
     -jar ${MARGINLOADER_LIB}/dave-margin-loader-1.0-SNAPSHOT-fat.jar

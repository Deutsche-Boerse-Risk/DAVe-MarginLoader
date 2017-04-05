#!/usr/bin/env bash

# Copy the DAVe binaries
cp -r -v ./target/dave-margin-loader-1.0-SNAPSHOT/dave-margin-loader-1.0-SNAPSHOT ./docker/dave-margin-loader-1.0-SNAPSHOT

docker login -e="$DOCKER_EMAIL" -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"
docker build -t dbgdave/dave-margin-loader:${CIRCLE_SHA1} ./docker/
docker tag -f dbgdave/dave-margin-loader:${CIRCLE_SHA1} docker.io/dbgdave/dave-margin-loader:${CIRCLE_SHA1}
docker push dbgdave/dave-margin-loader:${CIRCLE_SHA1}
docker tag -f dbgdave/dave-margin-loader:${CIRCLE_SHA1} docker.io/dbgdave/dave-margin-loader:${CIRCLE_BRANCH}
docker push dbgdave/dave-margin-loader:${CIRCLE_BRANCH}
